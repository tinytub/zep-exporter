//   Copyright 2016 DigitalOcean
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package collectors

import (
	"github.com/tinytub/zep-exporter/proto/ZPMeta"
	"github.com/tinytub/zep-exporter/zeppelin"
)

// Conn interface implements only necessary methods that are used
// in this repository of *rados.Conn. This keeps rest of the implementation
// clean and *rados.Conn doesn't need to show up everywhere (it being
// more of an implementation detail in reality). Also it makes mocking
// easier for unit-testing the collectors.
type Conn interface {
	//	MetaConn(string) (*zeppelin.Connection, error)
	//	NodeConns() map[string]*zeppelin.Connection
	ListMeta() (*ZPMeta.MetaCmdResponse, error)
	ListNode() (*ZPMeta.MetaCmdResponse, error)
	//	Shutdown()
	//	MonCommand([]byte) ([]byte, string, error)
}

// Verify that *rados.Conn implements Conn correctly.
var _ Conn = &zeppelin.Connection{}
