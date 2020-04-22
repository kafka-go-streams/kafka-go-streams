package streams

type Slice interface {
	Free()
	Data() []byte
	Size() int
	Exists() bool
}
