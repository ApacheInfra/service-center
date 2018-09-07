module github.com/apache/incubator-servicecomb-service-center

replace (
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b => github.com/istio/glog v0.0.0-20180224222734-2cc4b790554d
	go.uber.org/zap v1.9.0 => github.com/uber-go/zap v1.9.0
	golang.org/x/crypto v0.0.0-20180820150726-614d502a4dac => github.com/golang/crypto v0.0.0-20180820150726-614d502a4dac
	golang.org/x/net v0.0.0-20180824152047-4bcd98cce591 => github.com/golang/net v0.0.0-20180824152047-4bcd98cce591
	golang.org/x/sys v0.0.0-20180824143301-4910a1d54f87 => github.com/golang/sys v0.0.0-20180824143301-4910a1d54f87
	golang.org/x/text v0.0.0-20170627122817-6353ef0f9243 => github.com/golang/text v0.0.0-20170627122817-6353ef0f9243
	golang.org/x/time v0.0.0-20170424234030-8be79e1e0910 => github.com/golang/time v0.0.0-20170424234030-8be79e1e0910
	google.golang.org/genproto v0.0.0-20170531203552-aa2eb687b4d3 => github.com/google/go-genproto v0.0.0-20170531203552-aa2eb687b4d3
	google.golang.org/grpc v1.2.1-0.20170627165434-3c33c26290b7 => github.com/grpc/grpc-go v1.2.1-0.20170627165434-3c33c26290b7
	k8s.io/api v0.0.0-20180601181742-8b7507fac302 => github.com/kubernetes/api v0.0.0-20180601181742-8b7507fac302
	k8s.io/apimachinery v0.0.0-20180601181227-17529ec7eadb => github.com/kubernetes/apimachinery v0.0.0-20180601181227-17529ec7eadb
	k8s.io/client-go v2.0.0-alpha.0.0.20180817174322-745ca8300397+incompatible => github.com/kubernetes/client-go v2.0.0-alpha.0.0.20180817174322-745ca8300397+incompatible
)

require (
	github.com/Shopify/sarama v1.18.0 // indirect
	github.com/apache/thrift v0.0.0-20180125231006-3d556248a8b9 // indirect
	github.com/astaxie/beego v1.8.0
	github.com/beorn7/perks v0.0.0-20180321164747-3a771d992973 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/cockroachdb/cmux v0.0.0-20170110192607-30d10be49292 // indirect
	github.com/coreos/etcd v3.1.9+incompatible
	github.com/coreos/go-semver v0.2.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20180828140353-eee3db372b31 // indirect
	github.com/coreos/pkg v0.0.0-20180108230652-97fdf19511ea // v4
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/eapache/go-resiliency v1.1.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-chassis/paas-lager v0.0.0-20180905100939-eff93e5e67db
	github.com/go-logfmt/logfmt v0.3.0 // indirect
	github.com/go-mesh/openlogging v0.0.0-20180905092207-9cc15d7752d3 // indirect
	github.com/gogo/protobuf v1.1.1 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/protobuf v1.0.0
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c // indirect
	github.com/google/gofuzz v0.0.0-20170612174753-24818f796faf // indirect
	github.com/googleapis/gnostic v0.2.0 // indirect
	github.com/gorilla/websocket v1.2.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.1.1-0.20161105223513-84398b94e188 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/howeyc/gopass v0.0.0-20170109162249-bf9dde6d0d2c // indirect
	github.com/hpcloud/tail v1.0.0 // indirect
	github.com/imdario/mergo v0.3.6 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/json-iterator/go v1.1.5 // indirect
	github.com/karlseguin/ccache v2.0.3-0.20170217060820-3ba9789cfd2c+incompatible
	github.com/kr/logfmt v0.0.0-20140226030751-b84e30acd515 // indirect
	github.com/labstack/echo v3.2.2-0.20180316170059-a5d81b8d4a62+incompatible
	github.com/labstack/gommon v0.2.1 // indirect
	github.com/mattn/go-colorable v0.0.9 // indirect
	github.com/mattn/go-isatty v0.0.4 // indirect
	github.com/mattn/go-runewidth v0.0.3 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/natefinch/lumberjack v0.0.0-20170531160350-a96e63847dc3
	github.com/olekukonko/tablewriter v0.0.0-20180506121414-d4647c9c7a84
	github.com/onsi/ginkgo v1.6.0
	github.com/onsi/gomega v1.4.1
	github.com/opentracing-contrib/go-observer v0.0.0-20170622124052-a52f23424492 // indirect
	github.com/opentracing/opentracing-go v1.0.2
	github.com/openzipkin/zipkin-go-opentracing v0.3.3-0.20180123190626-6bb822a7f15f
	github.com/pierrec/lz4 v2.0.5+incompatible // indirect
	github.com/prometheus/client_golang v0.8.1-0.20170628125436-ab4214782d02
	github.com/prometheus/client_model v0.0.0-20170216185247-6f3806018612
	github.com/prometheus/common v0.0.0-20180801064454-c7de2306084e // indirect
	github.com/prometheus/procfs v0.0.0-20180725123919-05ee40e3a273 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20180503174638-e2704e165165 // indirect
	github.com/rs/cors v0.0.0-20170608165155-8dd4211afb5d // v1.1
	github.com/satori/go.uuid v1.1.0
	github.com/spf13/cobra v0.0.0-20170624150100-4d647c8944eb
	github.com/spf13/pflag v1.0.0
	github.com/ugorji/go v0.0.0-20170620104852-5efa3251c7f7 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v0.0.0-20170224212429-dcecefd839c4 // indirect
	github.com/widuu/gojson v0.0.0-20170212122013-7da9d2cd949b
	github.com/xiang90/probing v0.0.0-20160813154853-07dd2e8dfe18 // indirect
	go.uber.org/atomic v1.3.2 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.9.0
	golang.org/x/crypto v0.0.0-20180820150726-614d502a4dac // indirect
	golang.org/x/net v0.0.0-20180824152047-4bcd98cce591
	golang.org/x/sys v0.0.0-20180824143301-4910a1d54f87 // indirect
	golang.org/x/text v0.0.0-20170627122817-6353ef0f9243 // indirect
	golang.org/x/time v0.0.0-20170424234030-8be79e1e0910 // indirect
	google.golang.org/genproto v0.0.0-20170531203552-aa2eb687b4d3 // indirect
	google.golang.org/grpc v1.2.1-0.20170627165434-3c33c26290b7
	gopkg.in/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v2 v2.2.1 // indirect
	k8s.io/api v0.0.0-20180601181742-8b7507fac302
	k8s.io/apimachinery v0.0.0-20180601181227-17529ec7eadb
	k8s.io/client-go v2.0.0-alpha.0.0.20180817174322-745ca8300397+incompatible
)
