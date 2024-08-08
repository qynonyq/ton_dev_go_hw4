package app

type App struct {
	Cfg *Cfg
}

func InitApp() (*App, error) {
	cfg, err := initConfig()
	if err != nil {
		return nil, err
	}

	if err := initLogger(cfg.LogLevel); err != nil {
		return nil, err
	}

	if err := initDatabase(cfg.Postgres); err != nil {
		return nil, err
	}

	initTLB()

	return &App{Cfg: cfg}, nil
}
