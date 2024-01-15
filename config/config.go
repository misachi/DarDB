package config

type Config struct {
	bufferSize    uint64
	walBufferSize uint64
	dataPath      string
	// walPath       string
}

func NewConfig(path string, bufSz, walBufSz uint64) *Config {
	return &Config{
		dataPath: path,
		// walPath: walPath,
		bufferSize: bufSz,
		walBufferSize: walBufSz,
	}
}

func (c Config) BufferSize() uint64 {
	return c.bufferSize
}

func (c Config) WalBufferSize() uint64 {
	return c.walBufferSize
}

func (c Config) DataPath() string {
	return c.dataPath
}

// func (c Config) WalDataPath() string {
// 	return c.walPath
// }
