module github.com/CSUNetSec/bgpmon

replace github.com/CSUNetSec/netsec-protobufs v0.1.2-devel => ../netsec-protobufs

require (
	github.com/BurntSushi/toml v0.3.0
	github.com/CSUNetSec/bgpmon/v2 v2.0.0-20190215194558-4530641376cb
	github.com/CSUNetSec/netsec-protobufs v0.1.2-devel
	github.com/CSUNetSec/protoparse v0.1.1
	github.com/araddon/dateparse v0.0.0-20181123171228-21df004e09ca
	github.com/armon/go-radix v0.0.0-20170727155443-1fca145dffbc
	github.com/fsnotify/fsnotify v1.4.7
	github.com/golang/protobuf v1.2.0
	github.com/google/uuid v0.0.0-20171129191014-dec09d789f3d
	github.com/hashicorp/hcl v0.0.0-20180404174102-ef8a98b0bbce
	github.com/lib/pq v0.0.0-20180523175426-90697d60dd84
	github.com/magiconair/properties v1.8.0
	github.com/mitchellh/go-homedir v0.0.0-20180523094522-3864e76763d9
	github.com/mitchellh/mapstructure v0.0.0-20180511142126-bb74f1db0675
	github.com/pelletier/go-toml v1.2.0
	github.com/pkg/errors v0.8.0
	github.com/sirupsen/logrus v1.0.5
	github.com/spf13/afero v1.1.1
	github.com/spf13/cast v1.2.0
	github.com/spf13/cobra v0.0.3
	github.com/spf13/jwalterweatherman v0.0.0-20180109140146-7c0cea34c8ec
	github.com/spf13/pflag v1.0.1
	github.com/spf13/viper v1.0.2
	golang.org/x/crypto v0.0.0-20180621125126-a49355c7e3f8
	golang.org/x/net v0.0.0-20180702212446-ed29d75add3d
	golang.org/x/sys v0.0.0-20180704094941-151529c776cd
	golang.org/x/text v0.3.0
	google.golang.org/genproto v0.0.0-20180627194029-ff3583edef7d
	google.golang.org/grpc v1.13.0
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127
	gopkg.in/yaml.v2 v2.2.1
)
