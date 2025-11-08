package log

import (
	"fmt"
	"os"
	"path/filepath"

	api "github.com/kentakki416/proglog/api/v1"

	"google.golang.org/protobuf/proto"
)

// segment: ログストアのセグメントを管理する構造体
// セグメントは、ストアファイル（実際のデータ）とインデックスファイル（検索用のインデックス）を
// 組み合わせたログの基本単位。ディスク容量が有限なため、ログを複数のセグメントに分割して管理する。
// 各セグメントは baseOffset から始まる連続したオフセット範囲を担当する。
type segment struct {
	store      *store // ストアファイル（実際のレコードデータを保存）
	index      *index // インデックスファイル（オフセットとストア内位置の対応表）
	baseOffset uint64 // このセグメントの開始オフセット（例: 0, 1000, 2000）
	nextOffset uint64 // 次のレコードを追加する際の絶対オフセット（例: 0, 1001, 2001）
	config     Config // セグメントの設定（最大サイズなど）
}

// newSegment: 新しいセグメントを作成または既存のセグメントを開く
// セグメントは baseOffset をファイル名に含めることで識別される（例: "0.store", "0.index"）
// 引数:
//   - dir: セグメントファイルを保存するディレクトリ
//   - baseOffset: このセグメントの開始オフセット（セグメントを識別するための一意の値）
//   - c: セグメントの設定情報
//
// 戻り値:
//   - *segment: 初期化されたセグメント構造体
//   - error: エラーが発生した場合
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	// ストアファイルを開く、なければ作成
	// ファイル名: "{baseOffset}.store"（例: "0.store", "1000.store"）
	// O_RDWR: 読み書き可能、O_CREATE: 存在しなければ作成、O_APPEND: 追加モード
	storeFile, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// インデックスファイルを開く、なければ作成
	// ファイル名: "{baseOffset}.index"（例: "0.index", "1000.index"）
	indexFile, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// 既存のインデックスから最後のエントリを読み取り、nextOffset を決定
	// インデックスが空（新規セグメント）の場合は baseOffset から開始
	// 既存のセグメントの場合は、最後のオフセット + 1 から開始
	// 例: baseOffset = 1000, 最後のエントリの off = 99 の場合
	//     nextOffset = 1000 + 99 + 1 = 1100
	if off, _, err := s.index.Read(-1); err != nil {
		// インデックスが空（新規セグメント）の場合
		s.nextOffset = baseOffset
	} else {
		// 既存のセグメントの場合、最後のオフセット + 1 を設定
		// off は baseOffset からの相対位置なので、baseOffset + off + 1 が次のオフセット
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append: レコードをセグメントに追加する
// プロセス:
//  1. レコードにオフセットを設定
//  2. レコードを Protocol Buffers 形式にシリアライズ
//  3. ストアファイルにデータを追加し、ストア内の位置（pos）を取得
//  4. インデックスに「相対オフセット」と「ストア内位置」の対応を記録
//  5. nextOffset をインクリメント
//
// 引数:
//   - record: 追加するレコード（Offset フィールドは自動設定される）
//
// 戻り値:
//   - offset: 割り当てられたオフセット（例: 0, 1, 2, ... または 1000, 1001, 1002, ...）
//   - error: エラーが発生した場合
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	// 現在の nextOffset をレコードのオフセットとして使用
	cur := s.nextOffset
	record.Offset = cur

	// レコードを Protocol Buffers 形式にシリアライズ（バイナリ形式に変換）
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// ストアファイルにデータを追加し、ストア内の位置（pos）を取得
	// pos はストアファイル内のバイト位置（例: 0, 13, 26, ...）
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// インデックスに「相対オフセット」と「ストア内位置」の対応を記録
	// 相対オフセット = 絶対オフセット - baseOffset
	// 例: baseOffset = 1000, cur = 1005 の場合、相対オフセット = 5
	//     インデックスには (5, pos) が記録される
	if err = s.index.Write(uint32(s.nextOffset-uint64(s.baseOffset)), pos); err != nil {
		return 0, err
	}

	// 次のレコード用のオフセットをインクリメント
	s.nextOffset++
	return cur, nil
}

// Read: 指定されたオフセットのレコードを読み取る
// プロセス:
//  1. インデックスから、指定オフセットに対応するストア内位置（pos）を取得
//  2. ストアファイルから pos の位置からデータを読み取り
//  3. Protocol Buffers 形式からレコードにデシリアライズ
//
// 引数:
//   - off: 読み取るレコードのオフセット（絶対オフセット、例: 1005）
//
// 戻り値:
//   - *api.Record: 読み取ったレコード
//   - error: エラーが発生した場合（オフセットが見つからない場合など）
func (s *segment) Read(off uint64) (*api.Record, error) {
	// インデックスから、指定オフセットに対応するストア内位置を取得
	// インデックスには相対オフセットが記録されているため、絶対オフセットから baseOffset を引く
	// 例: baseOffset = 1000, off = 1005 の場合、相対オフセット = 5 で検索
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// ストアファイルから pos の位置からデータを読み取り
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	// Protocol Buffers 形式からレコードにデシリアライズ（バイナリ形式から構造体に変換）
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// IsMaxed: セグメントが最大サイズに達したかどうかをチェック
// セグメントが最大サイズに達した場合、新しいセグメントを作成する必要がある
// チェック項目:
//  1. ストアファイルのサイズが最大値を超えているか
//  2. インデックスファイルのサイズが最大値を超えているか
//  3. インデックスに新しいエントリを追加する余地がないか
//
// 戻り値:
//   - bool: 最大サイズに達している場合 true、まだ余裕がある場合 false
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes ||
		s.index.isMaxed()
}

// Remove: セグメントを削除する
// セグメントが不要になった場合（例: ログのローテーション時）に呼び出される
// プロセス:
//  1. セグメントを閉じる（リソースのクリーンアップ）
//  2. インデックスファイルを削除
//  3. ストアファイルを削除
//
// 戻り値:
//   - error: エラーが発生した場合
func (s *segment) Remove() error {
	// セグメントを閉じる（メモリマップの同期、ファイルのクローズなど）
	if err := s.Close(); err != nil {
		return err
	}

	// インデックスファイルを削除（例: "0.index"）
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	// ストアファイルを削除（例: "0.store"）
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// Close: セグメントを閉じてリソースをクリーンアップ
// インデックスとストアの両方を適切に閉じる
// プロセス:
//  1. インデックスを閉じる（メモリマップの同期、ファイルサイズの調整など）
//  2. ストアを閉じる（バッファのフラッシュ、ファイルのクローズ）
//
// 戻り値:
//   - error: エラーが発生した場合
func (s *segment) Close() error {
	// インデックスを閉じる（メモリマップの同期、ファイルサイズの調整）
	if err := s.index.Close(); err != nil {
		return err
	}

	// ストアを閉じる（バッファのフラッシュ、ファイルのクローズ）
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}
