package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// インデックスエントリの各フィールドのバイト幅を定義
const (
	offWidth uint64 = 4                   // 「レコードの論理番号」=オフセット（レコードの相対位置）を格納するためのバイト数（uint32 = 4バイト）
	posWidth uint64 = 8                   // 「ストアファイル内の物理位置」=ポジション（ストアファイル内の絶対位置）を格納するためのバイト数（uint64 = 8バイト）
	entWidth        = offWidth + posWidth // 「1つのデータの合計」=1つのインデックスエントリ全体（）のバイト数（4 + 8 = 12バイト）
)

// index: ログストアのインデックスを管理する構造体
// メモリマップドファイル（mmap）を使用して、高速なランダムアクセスを実現
// インデックスエントリの構造: [オフセット(4バイト)][ポジション(8バイト)] を繰り返し
type index struct {
	file *os.File    // インデックスファイルのファイルハンドル
	mmap gommap.MMap // メモリマップドファイル（インデックスファイルをメモリ上にマッピングして高速アクセスを実現）
	size uint64      // 現在のインデックスファイルの有効なデータサイズ（バイト単位）
}

// newIndex: 指定されたファイルからインデックスを作成
// インデックスファイルをメモリマップドファイルとして開き、高速な読み書きを可能にする
// 引数:
//   - f: インデックスファイルのファイルハンドル
//   - c: 設定情報（セグメントの最大インデックスサイズなど）
//
// 戻り値:
//   - *index: 初期化されたインデックス構造体
//   - error: エラーが発生した場合
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	// 既存ファイルのサイズを取得（既存のインデックスエントリがある場合に備える）
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// 現在のファイルサイズを有効なデータサイズとして記録
	idx.size = uint64(fi.Size())

	// ファイルを設定された最大サイズまで拡張（メモリマップのために事前にサイズを確保）
	// これにより、後でメモリマップする際に十分な領域が確保される
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	// ファイルをメモリにマッピング（読み書き可能、共有モード）
	// PROT_READ|PROT_WRITE: 読み取りと書き込みを許可
	// MAP_SHARED: メモリへの変更をファイルに反映（他のプロセスからも見える）
	if idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}
	return idx, nil
}

// Close: インデックスを閉じてリソースをクリーンアップ
// メモリマップの変更をファイルに同期し、ファイルサイズを実際のデータサイズに調整してから閉じる
// 戻り値:
//   - error: エラーが発生した場合
func (i *index) Close() error {
	// メモリマップの変更をファイルに同期的に書き込む（MS_SYNC: 同期的に書き込み）
	// これにより、メモリ上の変更が確実にディスクに反映される
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// ファイルの変更をディスクに同期的に書き込む
	if err := i.file.Sync(); err != nil {
		return err
	}

	// ファイルサイズを実際のデータサイズに調整（事前に拡張した領域を削除）
	// これにより、ファイルサイズが実際のデータサイズと一致する
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	// ファイルを閉じる
	return i.file.Close()
}

// Read: インデックスからエントリを読み取る
// 引数:
//   - in: 読み取るエントリのインデックス番号（-1の場合は最後のエントリを読み取る）
//
// 戻り値:
//   - out: オフセット（レコードの相対位置、uint32）
//   - pos: ポジション（ストアファイル内の絶対位置、uint64）
//   - err: エラーが発生した場合（io.EOF: インデックスが空、または範囲外）
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	// インデックスが空の場合はエラーを返す
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// in == -1 の場合は最後のエントリのインデックスを計算
	// 例: size = 24バイト、entWidth = 12バイトの場合、エントリ数は 24/12 = 2個
	//     最後のエントリのインデックスは 2 - 1 = 1（0始まり）
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	// 読み取るエントリのファイル内での開始位置を計算
	// 例: インデックス2の場合、2 * 12 = 24バイト目から読み始める
	pos = uint64(out) * entWidth

	// 読み取ろうとするエントリが有効なデータ範囲を超えている場合はエラーを返す
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	// メモリマップからオフセットを読み取る（最初の4バイト）
	// 例: mmap[24:28] から4バイト読み取り、uint32に変換
	out = enc.Uint32(i.mmap[pos : pos+offWidth])

	// メモリマップからポジションを読み取る（次の8バイト）
	// 例: mmap[28:36] から8バイト読み取り、uint64に変換
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])

	return out, pos, nil
}

// Write: インデックスに新しいエントリを書き込む
// 引数:
//   - off: オフセット（レコードの相対位置、uint32）
//   - pos: ポジション（ストアファイル内の絶対位置、uint64）
//
// 戻り値:
//   - error: エラーが発生した場合（io.EOF: インデックスが最大サイズに達している）
func (i *index) Write(off uint32, pos uint64) error {
	// インデックスが最大サイズに達している場合はエラーを返す
	if i.isMaxed() {
		return io.EOF
	}

	// メモリマップにオフセットを書き込む（現在のサイズ位置から4バイト）
	// 例: size = 24の場合、mmap[24:28] に4バイト書き込む
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)

	// メモリマップにポジションを書き込む（次の8バイト）
	// 例: size = 24の場合、mmap[28:36] に8バイト書き込む
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	// 有効なデータサイズを1エントリ分（12バイト）増やす
	i.size += uint64(entWidth)

	return nil
}

// isMaxed: インデックスが最大サイズに達したかどうかをチェック
// 新しいエントリを追加するための十分な領域があるかを確認
// 戻り値:
//   - bool: 最大サイズに達している場合true、まだ余裕がある場合false
func (i *index) isMaxed() bool {
	// メモリマップのサイズが、現在のサイズ + 新しいエントリのサイズより小さい場合は最大サイズに達している
	// 例: mmapサイズ = 100バイト、現在のサイズ = 96バイト、entWidth = 12バイトの場合
	//     96 + 12 = 108 > 100 なので、最大サイズに達している
	return uint64(len(i.mmap)) < i.size+entWidth
}

func (i *index) Name() string {
	return i.file.Name()
}
