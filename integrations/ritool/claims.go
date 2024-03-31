package main

import (
	"database/sql"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifregtypes13 "github.com/filecoin-project/go-state-types/builtin/v13/verifreg"
	"github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"
	verifreg2 "github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	"github.com/filecoin-project/lotus/chain/types"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/filecoin-project/lotus/lib/must"
	_ "github.com/mattn/go-sqlite3"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"strings"
)

var claimsExtendCmd = &cli.Command{
	Name:      "claims-to-extend",
	Usage:     "Extend claims",
	ArgsUsage: "[ribs db]",
	Flags: []cli.Flag{
		&cli.Int64Flag{
			Name:     "client-id",
			Required: true,
		},
	},
	Action: func(c *cli.Context) error {
		rdb, err := sql.Open("sqlite3", (c.Args().First()))
		if err != nil {
			return xerrors.Errorf("open db: %w", err)
		}

		client := abi.ActorID(c.Int64("client-id"))

		must.One(rdb.Exec("PRAGMA journal_mode=WAL;"))
		must.One(rdb.Exec("PRAGMA synchronous = normal"))

		var provs []address.Address

		{
			// SELECT DISTINCT provider_addr FROM deals WHERE sealed=1
			rows, err := rdb.Query("SELECT DISTINCT provider_addr FROM deals WHERE sealed=1")
			if err != nil {
				return xerrors.Errorf("query: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				var prov uint64
				if err := rows.Scan(&prov); err != nil {
					return xerrors.Errorf("scan: %w", err)
				}
				provs = append(provs, must.One(address.NewIDAddress(prov)))
			}
			if err := rows.Err(); err != nil {
				return xerrors.Errorf("rows: %w", err)
			}
			if err := rows.Close(); err != nil {
				return xerrors.Errorf("close: %w", err)
			}
		}

		fmt.Printf("Getting claims for %d providers\n", len(provs))

		chain, closer, err := cliutil.GetGatewayAPI(c)
		if err != nil {
			return xerrors.Errorf("getting gateway api: %w", err)
		}
		defer closer()

		ctx := cliutil.ReqContext(c)

		claimOurs := func(claim verifreg.Claim) bool {
			return claim.Client == client
		}

		var nclaims int
		claims := map[address.Address]map[verifreg.ClaimId]verifreg.Claim{}

		for n, prov := range provs {
			fmt.Printf("Getting claims for %s (%d/%d)\n", prov, n, len(provs))
			allProvClaims, err := chain.StateGetClaims(ctx, prov, types.EmptyTSK)
			if err != nil {
				return xerrors.Errorf("getting claims: %w", err)
			}

			for claimID, claim := range allProvClaims {
				if !claimOurs(claim) {
					continue
				}

				if _, ok := claims[prov]; !ok {
					claims[prov] = map[verifreg.ClaimId]verifreg.Claim{}
				}
				claims[prov][claimID] = claim
				nclaims++
			}
		}

		fmt.Printf("Got %d claims, creating messages\n", nclaims)

		tmax := abi.ChainEpoch(verifregtypes13.MaximumVerifiedAllocationTerm)
		params := verifreg.ExtendClaimTermsParams{}

		var totalGas int64
		totalFee := big.Zero()

		var mkMessage func(params verifreg.ExtendClaimTermsParams) (*types.Message, error)
		mkMessage = func(params verifreg.ExtendClaimTermsParams) (*types.Message, error) {
			clientAddr, err := address.NewIDAddress(uint64(client))
			if err != nil {
				return nil, err
			}

			enc, err := actors.SerializeParams(&params)
			if err != nil {
				return nil, err
			}

			m := &types.Message{
				To:     verifreg2.Address,
				From:   clientAddr,
				Method: verifreg2.Methods.ExtendClaimTerms,
				Params: enc,
				Value:  types.NewInt(0),
			}

			m, err = chain.GasEstimateMessageGas(ctx, m, nil, types.EmptyTSK)

			if (err != nil && strings.Contains(err.Error(), "call ran out of gas")) || m.GasLimit >= build.BlockGasLimit*4/5 {
				// estimate two messages
				p1 := params
				p2 := params

				p1.Terms = p1.Terms[:len(p1.Terms)/2]
				p2.Terms = p2.Terms[len(p2.Terms)/2:]

				_, err := mkMessage(p1)
				if err != nil {
					return nil, err
				}
				_, err = mkMessage(p2)
				if err != nil {
					return nil, err
				}
				return nil, nil
			}

			if err != nil {
				return nil, err
			}

			fmt.Printf("Estimate: %d Exts GasLimit=%d (%02d%% blk lim), GasFeeCap=%s, GasPremium=%s, Fee=%s\n", len(params.Terms),
				m.GasLimit, m.GasLimit*100/build.BlockGasLimit, m.GasFeeCap, m.GasPremium, types.FIL(m.RequiredFunds()))

			totalGas += m.GasLimit
			totalFee = types.BigAdd(totalFee, m.RequiredFunds())

			return m, nil
		}

		for provider, cls := range claims {
			mid, err := address.IDFromAddress(provider)
			if err != nil {
				return xerrors.Errorf("getting provider id: %w", err)
			}

			for claimId, claim := range cls {
				if claim.TermMax == tmax {
					continue
				}

				params.Terms = append(params.Terms, verifreg.ClaimTerm{
					Provider: abi.ActorID(mid),
					ClaimId:  claimId,
					TermMax:  tmax,
				})

				if len(params.Terms) >= 1000 {
					if _, err := mkMessage(params); err != nil {
						return err
					}
					params.Terms = nil
				}
			}
		}

		if len(params.Terms) > 0 {
			if _, err := mkMessage(params); err != nil {
				return err
			}
		}

		fmt.Printf("Total gas: %d (%.2f blks)\n", totalGas, float64(totalGas)/float64(build.BlockGasLimit))
		fmt.Printf("Total fee: %s\n", types.FIL(totalFee))

		return nil
	},
}
