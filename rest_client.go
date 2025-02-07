package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"time"
)

type RestClientConfig struct {
	URL       string `json:"url,omitempty" yaml:"url,omitempty"`
	Macaroon  string `json:"macaroon,omitempty" yaml:"macaroon,omitempty"`
	CACertB64 string `json:"ca_cert_b64,omitempty" yaml:"ca_cert_b64,omitempty"`
}

type RestClient struct {
	client http.Client
	config RestClientConfig
}

func NewRestClient(config RestClientConfig) (*RestClient, error) {
	certBytes, err := base64.StdEncoding.DecodeString(config.CACertB64)
	if err != nil {
		return nil, err
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(certBytes) {
		panic("Failed to append certificate")
	}

	return &RestClient{
		config: config,
		client: http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs:    certPool,
					MinVersion: tls.VersionTLS12,
				},
			},
			Timeout: 30 * time.Second,
		},
	}, nil
}

func (c *RestClient) LoopOutQuote(ctx context.Context, amt string) (LoopQuoteResponse, error) {
	respJson, err := execRestLoopCall[LoopQuoteResponse](
		c,
		ctx,
		http.MethodGet,
		fmt.Sprintf("/v1/loop/out/quote/%s", amt),
		nil,
	)
	if err != nil {
		return LoopQuoteResponse{}, err
	}
	return *respJson, nil
}

func (c *RestClient) LoopOut(ctx context.Context, req LoopOutRequest) (LoopOutResponse, error) {
	respJson, err := execRestLoopCall[LoopOutResponse](
		c,
		ctx,
		http.MethodPost,
		"/v1/loop/out",
		req,
	)
	if err != nil {
		return LoopOutResponse{}, err
	}
	return *respJson, nil
}

func (c *RestClient) ListSwaps(ctx context.Context, req ListSwapsRequest) (ListSwapResponse, error) {
	respJson, err := execRestLoopCall[ListSwapResponse](
		c,
		ctx,
		http.MethodGet,
		"/v1/loop/swaps",
		req,
	)
	if err != nil {
		return ListSwapResponse{}, err
	}
	return *respJson, nil
}

func (c *RestClient) LoopInfo(ctx context.Context) (LoopInfoResponse, error) {
	respJson, err := execRestLoopCall[LoopInfoResponse](
		c,
		ctx,
		http.MethodGet,
		"/v1/loop/info",
		nil,
	)
	if err != nil {
		return LoopInfoResponse{}, err
	}
	return *respJson, nil
}

func (c *RestClient) applyAccessToken(req *http.Request) {
	req.Header.Set("Grpc-Metadata-macaroon", c.config.Macaroon)
}

func execRestLoopCall[R any](c *RestClient, ctx context.Context, httpMethod, path string, payload any) (*R, error) {
	var body io.Reader = nil
	if payload != nil {
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewBuffer(payloadBytes)
	}

	req, err := http.NewRequestWithContext(ctx, httpMethod, fmt.Sprintf("%s%s", c.config.URL, path), body)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}
	c.applyAccessToken(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute lnd loop rest request %s: %w", path, err)
	}

	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusIMUsed {
		bodyBytes, parseErr := io.ReadAll(resp.Body)

		if parseErr == nil {
			return nil, fmt.Errorf("status: %s failed to execute lnd loop rest request %s server responded with: %s", resp.Status, path, string(bodyBytes))
		}

		return nil, fmt.Errorf("status: %s failed to execute lnd loop rest request %s", resp.Status, path)
	}

	responseJSON, err := parseRestLoopBodyBody[R](resp.Body)
	if err != nil {
		return nil, err
	}

	return responseJSON, err
}

func parseRestLoopBodyBody[R any](body io.ReadCloser) (*R, error) {
	result := new(R)
	err := json.NewDecoder(body).Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

type LoopQuoteResponse struct {
	SwapFeeSat      string `json:"swap_fee_sat"`
	PrepayAmtSat    string `json:"prepay_amt_sat"`
	HtlcSweepFeeSat string `json:"htlc_sweep_fee_sat"`
	SwapPaymentDest string `json:"swap_payment_dest"`
	CltvDelta       int    `json:"cltv_delta"`
	ConfTarget      int    `json:"conf_target"`
}

type LoopTotalFeeFlag string

const LoopTotalFeeFlagIgnorePrepayAmt LoopTotalFeeFlag = "LoopTotalFeeFlagIgnorePrepayAmt"

func (l *LoopQuoteResponse) GetSwapFeeSat() (int64, error) {
	i, err := strconv.ParseInt(l.SwapFeeSat, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("GetSwapFeeSat()")
	}
	return i, nil
}
func (l *LoopQuoteResponse) GetPrepayAmtSat() (int64, error) {
	i, err := strconv.ParseInt(l.PrepayAmtSat, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("GetPrepayAmtSat()")
	}
	return i, nil
}

func (l *LoopQuoteResponse) GetHtlcSweepFeeSat() (int64, error) {
	i, err := strconv.ParseInt(l.HtlcSweepFeeSat, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("GetHtlcSweepFeeSat()")
	}
	return i, nil
}

func (l *LoopQuoteResponse) TotalFeeSat(flags ...LoopTotalFeeFlag) (int64, error) {
	swapFee, err := strconv.ParseInt(l.SwapFeeSat, 10, 64)
	if err != nil {
		return 0, err
	}

	prepayAmt, err := strconv.ParseInt(l.PrepayAmtSat, 10, 64)
	if err != nil {
		return 0, err
	}

	htlcSweepFee, err := strconv.ParseInt(l.HtlcSweepFeeSat, 10, 64)
	if err != nil {
		return 0, err
	}

	if slices.Contains(flags, LoopTotalFeeFlagIgnorePrepayAmt) {
		prepayAmt = 0
	}

	return swapFee + prepayAmt + htlcSweepFee, nil
}

type LoopOutRequest struct {
	Amt                    int64  `json:"amt"`
	Dest                   string `json:"dest"`
	MaxSwapRoutingFeeSat   int64  `json:"max_swap_routing_fee,omitempty"`
	MaxPrepayRoutingFeeSat int64  `json:"max_prepay_routing_fee,omitempty"`
	MaxSwapFeeSat          int64  `json:"max_swap_fee,omitempty"`
	MaxPrepayAmtFee        int64  `json:"max_prepay_amt,omitempty"`
	SweepConfTarget        int32  `json:"sweep_conf_target,omitempty"`
}

type LoopOutResponse struct {
	ID               string `json:"id"`
	IdBytes          string `json:"id_bytes"`
	HtlcAddress      string `json:"htlc_address"`
	HtlcAddressP2Wsh string `json:"htlc_address_p2wsh"`
	HtlcAddressP2Tr  string `json:"htlc_address_p2tr"`
	ServerMessage    string `json:"server_message"`
}

type ListSwapsRequest struct {
}

type ListSwapResponse struct {
	Swaps []SwapEntry `json:"swaps"`
}

type SwapEntry struct {
	Amt              string `json:"amt"`
	Id               string `json:"id"`
	IdBytes          string `json:"id_bytes"`
	Type             string `json:"type"`
	State            string `json:"state"`
	FailureReason    string `json:"failure_reason"`
	InitiationTime   string `json:"initiation_time"`
	LastUpdateTime   string `json:"last_update_time"`
	HtlcAddress      string `json:"htlc_address"`
	HtlcAddressP2Wsh string `json:"htlc_address_p2wsh"`
	HtlcAddressP2Tr  string `json:"htlc_address_p2tr"`
	CostServer       string `json:"cost_server"`
	CostOnchain      string `json:"cost_onchain"`
	CostOffchain     string `json:"cost_offchain"`
	LastHop          string `json:"last_hop"`
	Label            string `json:"label"`
}

type LoopOutStats struct {
	PendingCount    string `json:"pending_count"`
	SuccessCount    string `json:"success_count"`
	FailCount       string `json:"fail_count"`
	SumPendingAmt   string `json:"sum_pending_amt"`
	SumSucceededAmt string `json:"sum_succeeded_amt"`
}
type LoopInStats LoopOutStats

type LoopInfoResponse struct {
	Version      string       `json:"version"`
	Network      string       `json:"network"`
	RpcListen    string       `json:"rpc_listen"`
	RestListen   string       `json:"rest_listen"`
	MacaroonPath string       `json:"macaroon_path"`
	TlsCertPath  string       `json:"tls_cert_path"`
	LoopOutStats LoopOutStats `json:"loop_out_stats"`
	LoopInStats  LoopInStats  `json:"loop_in_stats"`
}

func (i *LoopInfoResponse) IsPendingLoopOuts() bool {
	return i.LoopOutStats.PendingCount != "0"
}
