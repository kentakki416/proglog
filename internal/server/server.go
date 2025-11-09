package server

import (
	"context"

	api "github.com/kentakki416/proglog/api/v1"
	"google.golang.org/grpc"
)

// CommitLog: ログストアへの読み書きを行うインターフェース
// このインターフェースにより、サーバーは具体的なログ実装（例: log.Log）に依存せず、
// テスト時にはモックを注入できる（依存性注入のパターン）。
type CommitLog interface {
	Append(*api.Record) (uint64, error) // レコードをログに追加し、割り当てられたオフセットを返す
	Read(uint64) (*api.Record, error)   // 指定されたオフセットのレコードを読み取る
}

// Config: gRPC サーバーの設定
// サーバーが使用するログストア（CommitLog）を保持する。
type Config struct {
	CommitLog CommitLog // ログストアの実装（例: log.Log）
}

// grpcServer が api.LogServer インターフェースを実装していることをコンパイル時に確認
var _ api.LogServer = (*grpcServer)(nil)

// grpcServer: gRPC サーバーの実装
// Protocol Buffers で定義された Log サービスを実装する。
// UnimplementedLogServer を埋め込むことで、将来の API 変更に対して後方互換性を保つ。
type grpcServer struct {
	api.UnimplementedLogServer // 未実装のメソッドのデフォルト実装（後方互換性のため）
	*Config                    // サーバーの設定（埋め込みにより Config のフィールドに直接アクセス可能）
}

// NewGRPCServer: 新しい gRPC サーバーを作成する
// gRPC サーバーを初期化し、Log サービスを登録する。
// 引数:
//   - config: サーバーの設定（ログストアなど）
//
// 戻り値:
//   - *grpc.Server: 初期化された gRPC サーバー
//   - error: エラーが発生した場合
func NewGRPCServer(config *Config) (*grpc.Server, error) {
	// 新しい gRPC サーバーインスタンスを作成
	gsrv := grpc.NewServer()

	// grpcServer の実装を作成
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}

	// Log サービスを gRPC サーバーに登録
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

// newgrpcServer: grpcServer の実装を作成する（内部関数）
// 引数:
//   - config: サーバーの設定
//
// 戻り値:
//   - *grpcServer: 初期化された grpcServer
//   - error: エラーが発生した場合
func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{Config: config}
	return srv, nil
}

// Produce: レコードをログに追加する（単一リクエスト）
// クライアントから送信されたレコードをログストアに追加し、割り当てられたオフセットを返す。
// 引数:
//   - ctx: リクエストのコンテキスト（キャンセル、タイムアウトなど）
//   - req: 追加するレコードを含むリクエスト
//
// 戻り値:
//   - *api.ProduceResponse: 割り当てられたオフセットを含むレスポンス
//   - error: エラーが発生した場合
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	// ログストアにレコードを追加
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	// 割り当てられたオフセットを返す
	return &api.ProduceResponse{Offset: offset}, nil
}

// Consume: 指定されたオフセットのレコードを読み取る（単一リクエスト）
// クライアントが指定したオフセットのレコードをログストアから読み取り、返す。
// 引数:
//   - ctx: リクエストのコンテキスト（キャンセル、タイムアウトなど）
//   - req: 読み取るオフセットを含むリクエスト
//
// 戻り値:
//   - *api.ConsumeResponse: 読み取ったレコードを含むレスポンス
//   - error: エラーが発生した場合（オフセットが見つからない場合など）
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	// ログストアからレコードを読み取る
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	// 読み取ったレコードを返す
	return &api.ConsumeResponse{Record: record}, nil
}

// ProduceStream: ストリーミングでレコードをログに追加する
// クライアントから複数のレコードをストリーミングで受信し、順次ログストアに追加する。
// 各レコードの追加後、割り当てられたオフセットを即座にクライアントに返す。
// 引数:
//   - stream: 双方向ストリーム（クライアントからリクエストを受信、レスポンスを送信）
//
// 戻り値:
//   - error: エラーが発生した場合（ストリームの終了、エラーなど）
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		// クライアントからリクエストを受信
		req, err := stream.Recv()
		if err != nil {
			// ストリームの終了（EOF）またはエラー
			return err
		}

		// レコードをログストアに追加
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		// 割り当てられたオフセットをクライアントに送信
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// ConsumeStream: ストリーミングでレコードを読み取る
// 指定されたオフセットから順番にレコードを読み取り、ストリーミングでクライアントに送信する。
// 範囲外のオフセットに達するまで、またはクライアントがストリームを終了するまで続行する。
// 引数:
//   - req: 読み取りを開始するオフセットを含むリクエスト（req.Offset は読み取り中にインクリメントされる）
//   - stream: サーバーストリーム（クライアントにレスポンスを送信）
//
// 戻り値:
//   - error: エラーが発生した場合（ストリームの終了、エラーなど）
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			// クライアントがストリームを終了した場合、正常終了
			return nil
		default:
			// 現在のオフセットのレコードを読み取る
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
				// エラーなし: レコードが見つかった
			case api.ErrOffsetOutOfRange:
				// 範囲外のオフセット: 次のオフセットを試す（ログの末尾に達した可能性）
				continue
			default:
				// その他のエラー: ストリームを終了
				return err
			}

			// 読み取ったレコードをクライアントに送信
			if err = stream.Send(res); err != nil {
				return err
			}

			// 次のオフセットに進む
			req.Offset++
		}
	}
}
