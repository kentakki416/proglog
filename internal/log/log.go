package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/kentakki416/proglog/api/v1"
)

// Log: 複数のセグメントを管理するログストア
// ディスク容量が有限なため、ログを複数のセグメントに分割して管理する。
// 各セグメントは baseOffset から始まる連続したオフセット範囲を担当し、
// セグメントが最大サイズに達すると新しいセグメントが作成される。
type Log struct {
	mu sync.RWMutex // 読み書きロック（複数のgoroutineからの同時アクセス制御）

	Dir    string // セグメントファイルを保存するディレクトリ
	Config Config // ログストアの設定（セグメントの最大サイズなど）

	activeSegment *segment   // 現在書き込み中のセグメント（最新のセグメント）
	segments      []*segment // すべてのセグメント（baseOffset の昇順でソートされている）
}

// NewLog: 新しいログストアを作成または既存のログストアを開く
// 既存のセグメントファイルがある場合は、それらを読み込んでセグメントを復元する。
// 引数:
//   - dir: セグメントファイルを保存するディレクトリ
//   - c: ログストアの設定情報
//
// 戻り値:
//   - *Log: 初期化されたログストア構造体
//   - error: エラーが発生した場合
func NewLog(dir string, c Config) (*Log, error) {
	// デフォルト値の設定（設定が指定されていない場合）
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024 // デフォルト: 1KB
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024 // デフォルト: 1KB
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}

	// 既存のセグメントファイルを読み込んでセグメントを復元
	return l, l.setup()
}

// setup: 既存のセグメントファイルを読み込んでセグメントを復元する
// ディレクトリ内のファイル名から baseOffset を抽出し、セグメントを順番に開く。
// 既存のセグメントがない場合は、InitialOffset から新しいセグメントを作成する。
// 戻り値:
//   - error: エラーが発生した場合
func (l *Log) setup() error {
	// ディレクトリ内のすべてのファイルを読み込む
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	// ファイル名から baseOffset を抽出
	// ファイル名の形式: "{baseOffset}.store" または "{baseOffset}.index"
	// 例: "0.store", "0.index", "1000.store", "1000.index"
	var baseOffsets []uint64
	for _, file := range files {
		// ファイル名から拡張子を除いた部分を取得（例: "0.store" → "0"）
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		// 文字列を数値に変換（例: "0" → 0, "1000" → 1000）
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	// baseOffset を昇順にソート（セグメントを順番に処理するため）
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// 各 baseOffset に対してセグメントを作成
	// 注意: 同じ baseOffset に対して ".store" と ".index" の2つのファイルが存在するため、
	// 重複を避けるために i++ でスキップする
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffset は index と store の両方で重複しているため、重複をスキップ
		i++
	}

	// 既存のセグメントがない場合（新規ログストア）、InitialOffset から新しいセグメントを作成
	if l.segments == nil {
		if err = l.newSegment(
			l.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}
	}
	return nil
}

// Append: レコードをログストアに追加する
// アクティブセグメントが最大サイズに達している場合は、新しいセグメントを作成してから追加する。
// プロセス:
//  1. 現在の最高オフセットを取得
//  2. アクティブセグメントが最大サイズに達している場合、新しいセグメントを作成
//  3. アクティブセグメントにレコードを追加
//
// 引数:
//   - record: 追加するレコード（Offset フィールドは自動設定される）
//
// 戻り値:
//   - uint64: 割り当てられたオフセット（例: 0, 1, 2, ...）
//   - error: エラーが発生した場合
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 現在の最高オフセットを取得（新しいセグメントの baseOffset を決定するため）
	highestOffset, err := l.highestOffset()
	if err != nil {
		return 0, err
	}

	// アクティブセグメントが最大サイズに達している場合、新しいセグメントを作成
	// 新しいセグメントの baseOffset は、現在の最高オフセット + 1
	// 例: 現在の最高オフセットが 999 の場合、新しいセグメントの baseOffset は 1000
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(highestOffset + 1)
		if err != nil {
			return 0, err
		}
	}

	// アクティブセグメントにレコードを追加
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	return off, err
}

// Read: 指定されたオフセットのレコードを読み取る
// 指定されたオフセットが含まれるセグメントを検索し、そのセグメントからレコードを読み取る。
// 引数:
//   - off: 読み取るレコードのオフセット（絶対オフセット、例: 1005）
//
// 戻り値:
//   - *api.Record: 読み取ったレコード
//   - error: エラーが発生した場合（オフセットが見つからない場合など）
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// 指定されたオフセットが含まれるセグメントを検索
	// 条件: segment.baseOffset <= off < segment.nextOffset
	// 例: baseOffset = 1000, nextOffset = 2000 の場合、1000 <= off < 2000 の範囲を担当
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}

	// 該当するセグメントが見つからない場合、エラーを返す
	if s == nil {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	// セグメントからレコードを読み取る
	return s.Read(off)
}

// Close: ログストアを閉じてリソースをクリーンアップ
// すべてのセグメントを閉じる（メモリマップの同期、ファイルのクローズなど）。
// 戻り値:
//   - error: エラーが発生した場合
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// すべてのセグメントを閉じる
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove: ログストアを削除する
// すべてのセグメントを閉じた後、ディレクトリごと削除する。
// 戻り値:
//   - error: エラーが発生した場合
func (l *Log) Remove() error {
	// すべてのセグメントを閉じる
	if err := l.Close(); err != nil {
		return err
	}
	// ディレクトリごと削除（すべてのセグメントファイルが削除される）
	return os.RemoveAll(l.Dir)
}

// Reset: ログストアをリセットする
// すべてのセグメントを削除した後、新規ログストアとして初期化する。
// 戻り値:
//   - error: エラーが発生した場合
func (l *Log) Reset() error {
	// すべてのセグメントを削除
	if err := l.Remove(); err != nil {
		return err
	}
	// 新規ログストアとして初期化
	return l.setup()
}

// LowestOffset: ログストア内の最小オフセットを取得する
// 最初のセグメントの baseOffset を返す。
// 戻り値:
//   - uint64: 最小オフセット（最初のセグメントの baseOffset）
//   - error: エラーが発生した場合
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	// 最初のセグメントの baseOffset が最小オフセット
	return l.segments[0].baseOffset, nil
}

// HighestOffset: ログストア内の最大オフセットを取得する
// 最後のセグメントの nextOffset - 1 を返す（nextOffset は次のレコード用のオフセットなので、-1 する）。
// 戻り値:
//   - uint64: 最大オフセット（最後のセグメントの nextOffset - 1）
//   - error: エラーが発生した場合
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.highestOffset()
}

// highestOffset: ログストア内の最大オフセットを計算する（内部関数）
// 最後のセグメントの nextOffset - 1 を返す。
// nextOffset は次のレコード用のオフセットなので、実際の最後のレコードのオフセットは -1 する必要がある。
// 戻り値:
//   - uint64: 最大オフセット
//   - error: エラーが発生した場合
func (l *Log) highestOffset() (uint64, error) {
	// 最後のセグメントの nextOffset を取得
	off := l.segments[len(l.segments)-1].nextOffset
	// nextOffset が 0 の場合（セグメントが空）、0 を返す
	if off == 0 {
		return 0, nil
	}
	// nextOffset は次のレコード用のオフセットなので、-1 して実際の最後のレコードのオフセットを返す
	// 例: nextOffset = 1000 の場合、最後のレコードのオフセットは 999
	return off - 1, nil
}

// Truncate: 指定されたオフセットより前のセグメントを削除する
// ログのローテーションや古いデータの削除に使用される。
// 指定されたオフセット（lowest）より前のすべてのレコードを含むセグメントを削除する。
// 引数:
//   - lowest: 保持する最小オフセット（このオフセットより前のセグメントを削除）
//
// 戻り値:
//   - error: エラーが発生した場合
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 保持するセグメントのリスト
	var segments []*segment
	for _, s := range l.segments {
		// セグメントの nextOffset が lowest + 1 以下の場合、そのセグメントを削除
		// 例: lowest = 1000 の場合、nextOffset <= 1001 のセグメントを削除
		//     （nextOffset = 1001 は、最後のレコードのオフセットが 1000 を意味する）
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		// 保持するセグメントをリストに追加
		segments = append(segments, s)
	}
	// 保持するセグメントのリストで更新
	l.segments = segments
	return nil
}

// Reader: すべてのセグメントを順番に読み取る Reader を返す
// ログストア全体をストリームとして読み取る場合に使用される。
// すべてのセグメントのストアを順番に結合した Reader を返す。
// 戻り値:
//   - io.Reader: すべてのセグメントを順番に読み取る Reader
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// すべてのセグメントのストアから Reader を作成
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	// 複数の Reader を順番に結合した Reader を返す
	return io.MultiReader(readers...)
}

// originReader: ストアから順番に読み取る Reader
// ストアファイルの先頭から順番に読み取るための Reader 実装。
type originReader struct {
	*store       // ストアファイル
	off    int64 // 現在の読み取り位置（バイト位置）
}

// Read: ストアからデータを読み取る
// io.Reader インターフェースの実装。
// 引数:
//   - p: 読み取ったデータを格納するバッファ
//
// 戻り値:
//   - int: 読み取ったバイト数
//   - error: エラーが発生した場合
func (o *originReader) Read(p []byte) (int, error) {
	// 現在の位置からデータを読み取る
	n, err := o.ReadAt(p, o.off)
	// 読み取り位置を進める
	o.off += int64(n)
	return n, err
}

// newSegment: 新しいセグメントを作成してログストアに追加する
// 指定された baseOffset で新しいセグメントを作成し、セグメントリストに追加する。
// 新しく作成されたセグメントがアクティブセグメントになる。
// 引数:
//   - off: 新しいセグメントの baseOffset
//
// 戻り値:
//   - error: エラーが発生した場合
func (l *Log) newSegment(off uint64) error {
	// 新しいセグメントを作成
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	// セグメントリストに追加
	l.segments = append(l.segments, s)
	// 新しく作成されたセグメントをアクティブセグメントに設定
	l.activeSegment = s
	return nil
}
