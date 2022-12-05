package zfs

type Option func(*zfs)

func WithSudo() Option {
	return func(z *zfs) {
		z.sudo = true
	}
}

func WithExecutor(exec Executor) Option {
	return func(z *zfs) {
		z.exec = exec
	}
}

func WithLogger(logger Logger) Option {
	return func(z *zfs) {
		z.logger = logger
	}
}
