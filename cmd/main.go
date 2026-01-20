package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

func main() {
	logger.Info("Hello from 'Try NATS'!")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tempDir, err := os.MkdirTemp("", "try-nats")
	if err != nil {
		logger.Error("create temp dir",
			slog.String("name", tempDir),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	natsSrv, err := natsserver.NewServer(&natsserver.Options{
		JetStream: true,
		StoreDir:  tempDir, // required for JetStream persistence
	})
	if err != nil {
		logger.Error("create NATS server",
			slog.String("name", tempDir),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	natsSrv.Start()
	//defer natsSrv.Shutdown()

	readyTimeout := 5 * time.Second
	if !natsSrv.ReadyForConnections(readyTimeout) {
		logger.Error("NATS server not ready", slog.Duration("timeout", readyTimeout))
		os.Exit(1)
	}

	// ----

	natsConn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		logger.Error("connect to NATS", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer natsConn.Close()

	consumerConfig := make([]jetstream.ConsumerConfig, 3)
	for i := 0; i < len(consumerConfig); i++ {
		consumerConfig[i] = jetstream.ConsumerConfig{
			Durable:   "workers", // same name = shared cursor
			AckPolicy: jetstream.AckAllPolicy,
		}
	}

	ds, err := bootstrap(ctx, opts{
		conn:            natsConn,
		streamName:      "EVENTS",
		subjects:        []string{"events.>"},
		producerCount:   2,
		consumerConfigs: consumerConfig,
	})
	if err != nil {
		logger.Error("bootstrap", slog.String("error", err.Error()))
		os.Exit(1)
	}

	for i, p := range ds.publishers {
		publisherID := fmt.Sprintf("pub_%d", i)
		subject := fmt.Sprintf("events.basic_%d", i)
		go func() {
			logger := logger.With(
				slog.String("publisher_id", publisherID),
				slog.String("subject", subject),
			)
			logger.Info("starting publisher")
			if err := publish(ctx, logger, p, subject, publisherID); err != nil {
				logger.Error("publish", slog.String("error", err.Error()))
			}
			logger.Info("publishing stopped")
		}()
	}

	for i, p := range ds.consumers {
		consumerID := fmt.Sprintf("cons_%d", i)
		go func() {
			logger := logger.With(slog.String("consumer_id", consumerID))
			logger.Info("starting consumer")
			if err := consume(ctx, logger, p); err != nil {
				logger.Error("publish", slog.String("error", err.Error()))
			}
			logger.Info("consumption stopped")
		}()
	}

	<-ctx.Done()
}

type deps struct {
	publishers []jetstream.Publisher
	consumers  []jetstream.Consumer
}

type opts struct {
	conn            *nats.Conn
	streamName      string
	subjects        []string
	producerCount   int
	consumerConfigs []jetstream.ConsumerConfig
}

func bootstrap(ctx context.Context, opts opts) (deps, error) {
	adminJs, err := jetstream.New(opts.conn)
	if err != nil {
		return deps{}, fmt.Errorf("create JetStream instance: %w", err)
	}

	_, err = adminJs.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     opts.streamName,
		Subjects: opts.subjects,
	})

	if err != nil {
		return deps{}, fmt.Errorf("create stream: %w", err)
	}

	var (
		errs       = make([]error, 0, 5)
		publishers = make([]jetstream.Publisher, 0, opts.producerCount)
		consumers  = make([]jetstream.Consumer, 0, len(opts.consumerConfigs))
	)

	for i := 0; i < opts.producerCount; i++ {
		js, err := jetstream.New(opts.conn)
		if err != nil {
			errs = append(errs, fmt.Errorf("create JetStream instance: %w", err))
			continue
		}

		publishers = append(publishers, js)
	}

	for _, conf := range opts.consumerConfigs {
		js, err := jetstream.New(opts.conn)
		if err != nil {
			errs = append(errs, fmt.Errorf("create JetStream instance: %w", err))
			continue
		}

		cons, err := js.CreateOrUpdateConsumer(ctx, opts.streamName, conf)
		if err != nil {
			errs = append(errs, fmt.Errorf("create consumer: %w", err))
		}

		consumers = append(consumers, cons)

	}

	if err := errors.Join(errs...); err != nil {
		return deps{}, err
	}

	return deps{
		publishers: publishers,
		consumers:  consumers,
	}, nil
}

func publish(
	ctx context.Context, logger *slog.Logger, pub jetstream.Publisher, subject string, messagePrefix string,
) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("stopping publisher")
			return nil
		case t, ok := <-ticker.C:
			if !ok {
				return nil
			}

			msg := fmt.Sprintf("%s-%s", messagePrefix, t.Format(time.RFC3339))
			if _, err := pub.Publish(ctx, subject, []byte(msg)); err != nil {
				slog.Error("publish message", slog.String("error", err.Error()))
				continue
			}

			logger.Info("published", slog.String("message", msg))
		}
	}
}

func consume(ctx context.Context, logger *slog.Logger, cons jetstream.Consumer) error {
	consumeContext, err := cons.Consume(func(msg jetstream.Msg) {
		logger.Info(
			"consumed",
			slog.String("subject", msg.Subject()),
			slog.String("message", string(msg.Data())),
		)
	})
	if err != nil {
		return fmt.Errorf("consume message: %w", err)
	}
	<-ctx.Done()
	consumeContext.Stop()
	return nil
}
