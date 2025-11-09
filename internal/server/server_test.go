package server

import (
	"context"
	"net"
	"os"
	"testing"

	api "github.com/kentakki416/proglog/api/v1"
	"github.com/kentakki416/proglog/internal/config"
	"github.com/kentakki416/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestServer: gRPC サーバーの統合テスト
// 複数のテストシナリオを実行し、サーバーの基本的な機能を検証する。
// テストシナリオ:
//   - produce/consume: 単一のレコードの追加と読み取り
//   - produce/consume stream: ストリーミングでのレコードの追加と読み取り
//   - consume past log boundary: 範囲外のオフセットでのエラーハンドリング
func TestServer(t *testing.T) {
	// テストシナリオをマップで定義し、順番に実行
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"consume past log boundary fails":                     testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			// 各テストシナリオごとに新しいサーバーとクライアントをセットアップ
			client, config, teardown := setupTest(t, nil)
			defer teardown() // テスト終了時にリソースをクリーンアップ
			fn(t, client, config)
		})
	}
}

// setupTest: テスト用の gRPC サーバーとクライアントをセットアップする
// 一時的なディレクトリにログストアを作成し、gRPC サーバーを起動してクライアント接続を確立する。
// 引数:
//   - t: テストヘルパー
//   - fn: オプションの設定関数（Config をカスタマイズする場合に使用）
//
// 戻り値:
//   - client: gRPC クライアント（サーバーと通信するため）
//   - cfg: サーバーの設定
//   - teardown: リソースをクリーンアップする関数（defer で呼び出す）
func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	// ランダムなポートで TCP リスナーを作成（":0" で自動割り当て）
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// TLS証明書が存在するかチェック
	_, caErr := os.Stat(config.CAFile)
	_, certErr := os.Stat(config.ServerCertFile)
	_, keyErr := os.Stat(config.ServerKeyFile)

	var clientCreds credentials.TransportCredentials
	var serverCreds credentials.TransportCredentials

	// 証明書が存在する場合、TLSを使用
	if caErr == nil && certErr == nil && keyErr == nil {
		clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CAFile: config.CAFile,
		})
		require.NoError(t, err)
		clientCreds = credentials.NewTLS(clientTLSConfig)

		serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			ServerAddress: l.Addr().String(),
		})
		require.NoError(t, err)
		serverCreds = credentials.NewTLS(serverTLSConfig)
	} else {
		// 証明書が存在しない場合、テスト用に insecure な認証情報を使用
		clientCreds = insecure.NewCredentials()
		serverCreds = insecure.NewCredentials()
	}

	cc, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(clientCreds))
	require.NoError(t, err)

	// gRPC クライアントを作成
	client = api.NewLogClient(cc)

	// テスト用の一時ディレクトリを作成（テスト終了時に削除される）
	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	// ログストアを作成
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// サーバーの設定を作成
	cfg = &Config{
		CommitLog: clog,
	}
	// オプションの設定関数が提供されている場合、実行する
	if fn != nil {
		fn(cfg)
	}

	// gRPC サーバーを作成
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	// バックグラウンドで gRPC サーバーを起動
	go func() {
		server.Serve(l)
	}()

	// クリーンアップ関数を返す（defer で呼び出される）
	return client, cfg, func() {
		cc.Close()    // クライアント接続を閉じる
		server.Stop() // サーバーを停止
		l.Close()     // リスナーを閉じる
		clog.Remove() // ログストアのディレクトリを削除
	}
}

// testProduceConsume: 単一のレコードの追加と読み取りをテストする
// レコードを追加し、割り当てられたオフセットで読み取り、値とオフセットが一致することを確認する。
// 引数:
//   - t: テストヘルパー
//   - client: gRPC クライアント
//   - config: サーバーの設定（このテストでは使用しない）
func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	// 追加するレコードを定義
	want := &api.Record{
		Value: []byte("hello world"),
	}

	// レコードをログに追加
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)
	// 割り当てられたオフセットを期待値に設定
	want.Offset = produce.Offset

	// 追加したレコードを読み取る
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)

	// 読み取ったレコードの値とオフセットが期待値と一致することを確認
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

// testConsumePastBoundary: 範囲外のオフセットでのエラーハンドリングをテストする
// 存在しないオフセット（最後のレコードのオフセット + 1）で読み取りを試み、
// 適切なエラーコード（codes.OutOfRange）が返されることを確認する。
// 引数:
//   - t: テストヘルパー
//   - client: gRPC クライアント
//   - config: サーバーの設定（このテストでは使用しない）
func testConsumePastBoundary(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	// まず1つのレコードを追加（オフセット 0 が割り当てられる）
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	// 存在しないオフセット（最後のレコードのオフセット + 1）で読み取りを試みる
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	// レコードが返されないことを確認
	if consume != nil {
		t.Fatal("consume not nil")
	}

	// エラーコードが codes.OutOfRange であることを確認
	got := status.Code(err)
	want := codes.OutOfRange
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

// testProduceConsumeStream: ストリーミングでのレコードの追加と読み取りをテストする
// 複数のレコードをストリーミングで追加し、その後ストリーミングで読み取り、
// すべてのレコードが正しい順序とオフセットで処理されることを確認する。
// 引数:
//   - t: テストヘルパー
//   - client: gRPC クライアント
//   - config: サーバーの設定（このテストでは使用しない）
func testProduceConsumeStream(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	// 追加するレコードのリストを定義
	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}

	// ストリーミングでレコードを追加
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		// 各レコードを順番に送信
		for offset, record := range records {
			// レコードをストリームに送信
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)

			// サーバーから割り当てられたオフセットを受信
			res, err := stream.Recv()
			require.NoError(t, err)

			// 割り当てられたオフセットが期待値と一致することを確認
			if res.Offset != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.Offset,
					offset,
				)
			}
		}
	}

	// ストリーミングでレコードを読み取る
	{
		stream, err := client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{Offset: 0}, // オフセット 0 から読み取り開始
		)
		require.NoError(t, err)

		// 各レコードを順番に受信
		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)

			// 読み取ったレコードの値とオフセットが期待値と一致することを確認
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}
