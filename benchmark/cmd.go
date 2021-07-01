package bench

type Cmd struct {
	Name  string
	Key   string
	Value string
	Error error
}

//客户端接口
type Client interface {
	Run(*Cmd)
	PipelinedRun([]*Cmd)
}

//根据相应的类型创建指定的客户端
func New(typ, server string) Client {
	if typ == "redis" {
		return NewRedisClient(server)
	}
	if typ == "http" {
		return newHTTPClient(server)
	}
	if typ == "tcp" {
		return newTCPClient(server)
	}
	panic("unknown client type " + typ)
}
