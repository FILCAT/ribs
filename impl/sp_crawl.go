package impl

import cliutil "github.com/filecoin-project/lotus/cli/util"

func (r *ribs) spCrawler() {
	gwapi, closer, err := cliutil.GetGatewayAPI()
	if err != nil {
		panic(err)
	}

	for {

	}
}
