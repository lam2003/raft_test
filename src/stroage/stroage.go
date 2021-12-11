package stroage

type Stroage interface {
	Set(key, value string)
	Get(key string) string
}
