package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// BigEndian: ネットワークバイトオーダーで統一（異なるアーキテクチャ間での互換性確保）
var (
	enc = binary.BigEndian
)

// uint64でレコード長を格納（最大18.4EBまで対応可能）
const (
	lenWidth = 8
)

// store: ファイルベースのログストレージ
type store struct {
	*os.File               // 埋め込みでos.Fileのメソッドを直接使用
	mu       sync.Mutex    // 並行アクセス制御（複数goroutineからの同時アクセス防止）
	buf      *bufio.Writer // バッファリングでI/O性能向上
	size     uint64        // 現在のファイルサイズ（次のレコードの開始位置計算用）
}

// newStore: ファイルからstoreインスタンスを作成
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// 既存ファイルのサイズを取得（次のレコードの開始位置を決定するため）
	size := uint64(fi.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append: レコードをバッファに追加（ファイルには書き込まない）
// レコード構造: [長さ情報(8バイト)][データ] - 可変長データの境界を明確にするため
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size

	// 長さ情報をバイナリ形式で書き込み（可変長データの境界を明確にするため）
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// 実際のデータを書き込み
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)

	return uint64(w), pos, nil
}

// Read: 指定位置からレコードを読み取り
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// バッファをフラッシュ（最新データを確実にファイルに反映するため）
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// 長さ情報を読み取り
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// データサイズを取得して実際のデータを読み取り
	recordSize := enc.Uint64(size)
	b := make([]byte, recordSize)
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// ReadAt: io.ReaderAtインターフェースの実装
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// バッファをフラッシュ（最新データを確実にファイルに反映するため）
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

// Close: リソースのクリーンアップ
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// バッファをフラッシュ（データ損失を防ぐため）
	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}
