package cctbk

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/spf13/cast"
	"math"
	"reflect"
	"strings"
	"sync"
)

var (
	esTbIndex  string = "tbk_tb"
	esJdIndex  string = "tbk_jd"
	esPddIndex string = "tbk_pdd"
	esType     string = "goods"
)

type EsSearch struct {
	Host         string
	AuthName     string
	AuthPassword string
	client       *elastic.Client
}

// es连接
func (e *EsSearch) GetNewEsClient() *elastic.Client{
	// 创建ES client用于后续操作ES,
	client, err := elastic.NewClient(
		// 关闭 sniffing 模式被启用（默认启用
		elastic.SetSniff(false),
		// 设置ES服务地址，支持多个地址
		elastic.SetURL(e.Host),
		// 设置基于http base auth验证的账号和密码
		elastic.SetBasicAuth(e.AuthName, e.AuthPassword),
		// 启用gzip压缩
		elastic.SetGzip(true),
	)
	if err != nil {
		panic(err)
	}
	return client
}

// es搜索条件统一处理
func (e *EsSearch) QueryEsSearch(req *EsRequest) *EsReturnData {
	out := new(EsReturnData)
	outRecommend := new(EsReturnData)

	var wgEsSearch sync.WaitGroup
	wgEsSearch.Add(2)

	// 精准搜索
	go func() {
		defer func() {
			wgEsSearch.Done()
		}()
		out = e.QuerySearch(req)
	}()
	// 推荐，模糊匹配
	go func() {
		defer func() {
			wgEsSearch.Done()
		}()
		if req.Recommend == 0 {
			outRecommend = e.QueryEsLikeSearch(req)
		}
	}()
	wgEsSearch.Wait()

	if len(out.Data) > 0 {
		return out
	}
	return outRecommend
}

// es精准匹配
func (e *EsSearch) QuerySearch(req *EsRequest) *EsReturnData {
	out := new(EsReturnData)
	client := e.GetNewEsClient()

	// A、参数处理
	if len(strings.ReplaceAll(req.Keyword, " ", "")) < 1 {
		out.Code = 400
		out.Msg = "搜索词缺失"
		return out
	}
	if req.From < 1 {
		req.From = 1
	}
	if req.Size < 1 || req.Size > 50 {
		req.Size = 10
	}

	// B、es参数拼接
	q := elastic.NewBoolQuery()
	keywords := strings.Split(req.Keyword, " ")
	for _, kw := range keywords {
		q.Must(elastic.NewMatchPhraseQuery("title", kw))
	}
	if len(req.MaxPrice) > 0 { // 价格区间
		fmt.Println("price-", req.MinPrice, req.MaxPrice)
		q.Must(elastic.NewRangeQuery("price_end").Gt(req.MinPrice).Lte(req.MaxPrice))
	}

	search := client.Search(esTbIndex, esJdIndex, esPddIndex).Query(q).MinScore(1)
	allEsIndexStr := esTbIndex + "," + esJdIndex + "," + esPddIndex
	if strings.Count(allEsIndexStr, req.EsIndex) == 1 { // 单独查询
		search = client.Search(req.EsIndex).Query(q).MinScore(1)
	}

	// "排序方式: 默认：销量降序 10-升序 ，1.券后价排序;2.券面额排序;3.佣金排序;4.佣金率排序;"
	switch req.Sort {
	case "1":
		search.SortBy(elastic.NewFieldSort("price_end").Desc(), elastic.NewScoreSort())
	case "11":
		search.SortBy(elastic.NewFieldSort("price_end").Asc(), elastic.NewScoreSort())
	case "2":
		search.SortBy(elastic.NewFieldSort("coupon_money").Desc(), elastic.NewScoreSort())
	case "22":
		search.SortBy(elastic.NewFieldSort("coupon_money").Asc(), elastic.NewScoreSort())
	case "3":
		search.SortBy(elastic.NewFieldSort("tk_money").Desc(), elastic.NewScoreSort())
	case "33":
		search.SortBy(elastic.NewFieldSort("tk_money").Asc(), elastic.NewScoreSort())
	case "4":
		search.SortBy(elastic.NewFieldSort("tk_rates").Desc(), elastic.NewScoreSort())
	case "44":
		search.SortBy(elastic.NewFieldSort("tk_rates").Asc(), elastic.NewScoreSort())
	case "10":
		search.SortBy(elastic.NewFieldSort("total_sale").Asc(), elastic.NewScoreSort())
	default:
		search.SortBy(elastic.NewFieldSort("total_sale").Desc(), elastic.NewScoreSort())
	}

	res, err := search.From(req.From).Size(req.Size).Do(context.Background())
	if err != nil {
		out.Code = 400
		out.Msg = "查询出错了"
		return out
	}
	out = formatEs(res)
	out.TotalPage = cast.ToInt(math.Ceil(cast.ToFloat64(cast.ToInt(out.Total) / req.Size)))
	out.From = req.From
	out.Size = req.Size

	return out
}

// es模糊查询
func (e *EsSearch) QueryEsLikeSearch(req *EsRequest) *EsReturnData {
	out := new(EsReturnData)
	client := e.GetNewEsClient()

	// A、参数处理
	if len(strings.ReplaceAll(req.Keyword, " ", "")) < 1 {
		out.Code = 400
		out.Msg = "搜索词缺失"
		return out
	}
	if req.From < 1 {
		req.From = 1
	}
	if req.Size < 1 || req.Size > 50 {
		req.Size = 10
	}

	// B、es参数拼接
	q := elastic.NewBoolQuery()
	q.Should(
		elastic.NewMatchQuery("title", req.Keyword),
	)
	if len(req.MaxPrice) > 0 { // 价格区间
		q.Must(elastic.NewRangeQuery("price_end").Gt(req.MinPrice).Lte(req.MaxPrice))
	}

	search := client.Search(esTbIndex, esJdIndex, esPddIndex).Query(q).MinScore(1)
	allEsIndexStr := esTbIndex + "," + esJdIndex + "," + esPddIndex
	if strings.Count(allEsIndexStr, req.EsIndex) == 1 { // 单独查询
		search = client.Search(req.EsIndex).Query(q).MinScore(1)
	}

	// 排序只用score,其他排序会影响结果
	search.SortBy(elastic.NewScoreSort())

	res, err := search.From(req.From).Size(req.Size).Do(context.Background())
	if err != nil {
		out.Code = 400
		out.Msg = "查询出错了"
		return out
	}
	out = formatEs(res)
	out.TotalPage = cast.ToInt(math.Ceil(cast.ToFloat64(cast.ToInt(out.Total) / req.Size)))
	out.From = req.From
	out.Size = req.Size
	out.IsRecommend = 1

	return out
}

// 格式化es数据
func formatEs(res *elastic.SearchResult) *EsReturnData {
	esData := new(EsReturnData)
	var typ CommonEsData
	for _, item := range res.Each(reflect.TypeOf(typ)) { //从搜索结果中取数据的方法
		t := item.(CommonEsData)
		esData.Data = append(esData.Data, t)
	}
	esData.Total = res.Hits.TotalHits.Value

	return esData
}

// es入参
type EsRequest struct {
	Keyword   string
	Sort      string
	From      int
	Size      int
	MinPrice  string
	MaxPrice  string
	EsIndex   string
	Recommend int
}

// es通用数据结构
type CommonEsData struct {
	GoodsFrom       string `json:"goods_from"`
	GoodsId         string `json:"goods_id"`
	GoodsSign       string `json:"goods_sign"`
	Title           string `json:"title"`
	TitleShort      string `json:"title_short"`
	Description     string `json:"description"`
	Price           string `json:"price"`
	PriceEnd        string `json:"price_end"`
	TotalSale       string `json:"total_sale"`
	TodaySale       string `json:"today_sale"`
	Cid             string `json:"cid"`
	ShopType        string `json:"shop_type"`
	Pic             string `json:"pic"`
	PicAll          string `json:"pic_all"`
	CouponStartTime string `json:"coupon_start_time"`
	CouponEndTime   string `json:"coupon_end_time"`
	CouponNum       string `json:"coupon_num"`
	CouponSurplus   string `json:"coupon_surplus"`
	CouponMoney     string `json:"coupon_money"`
	TkRates         string `json:"tk_rates"`
	TkMoney         string `json:"tk_money"`
}

type EsReturnData struct {
	Code        int            `json:"code"`
	Msg         string         `json:"msg"`
	Total       int64          `json:"total"`
	TotalPage   int            `json:"total_page"`
	From        int            `json:"from"`
	Size        int            `json:"size"`
	IsRecommend int            `json:"is_recommend"`
	RequestTime string         `json:"request_time"`
	Data        []CommonEsData `json:"data"`
}

type TaoBaoHdk struct {
	Code  int             `json:"code"`
	MinId int             `json:"min_id"`
	Msg   string          `json:"msg"`
	Data  []TaoBaoHdkData `json:"data"`
}
type TaoBaoHdkData struct {
	ItemId          string `json:"itemid"`
	ItemTitle       string `json:"itemtitle"`
	ItemShortTitle  string `json:"itemshorttitle"`
	ItemDesc        string `json:"itemdesc"`
	ItemPrice       string `json:"itemprice"`
	ItemSale        string `json:"itemsale"`
	ItemSale2       string `json:"itemsale2"`
	TodaySale       string `json:"todaysale"`
	ItemPic         string `json:"itempic"`
	TaoBaoImage     string `json:"taobao_image"`
	FqCat           string `json:"fqcat"`
	ItemEndPrice    string `json:"itemendprice"`
	ShopType        string `json:"shoptype"`
	CouponMoney     string `json:"couponmoney"`
	IsBrand         string `json:"is_brand"`
	GuideArticle    string `json:"guide_article"`
	ShopName        string `json:"shopname"`
	TkRates         string `json:"tkrates"`
	TkMoney         string `json:"tkmoney"`
	CouponSurplus   string `json:"couponsurplus"`
	CouponNum       string `json:"couponnum"`
	CouponExplain   string `json:"couponexplain"`
	CouponStartTime string `json:"couponstarttime"`
	CouponEndTime   string `json:"couponendtime"`
	Discount        string `json:"discount"`
	Deposit         string `json:"deposit"`
	DepositDeduct   string `json:"deposit_deduct"`
	CouponInfo      string `json:"couponinfo"`
}

type JdHdk struct {
	Code  int         `json:"code"`
	MinId int         `json:"min_id"`
	Msg   string      `json:"msg"`
	Data  []JdHdkData `json:"data"`
}
type JdHdkData struct {
	SkuId           string `json:"skuid"`
	GoodsName       string `json:"goodsname"`
	GoodsNameShort  string `json:"goodsnameshort"`
	GoodsDesc       string `json:"goodsdesc"`
	ItemPrice       string `json:"itemprice"`
	ItemSale        string `json:"itemsale"`
	ItemSale2       string `json:"itemsale2"`
	TodaySale       string `json:"todaysale"`
	ItemPic         string `json:"itempic"`
	JdImage         string `json:"jd_image"`
	Cid             string `json:"cid"`
	ItemEndPrice    string `json:"itemendprice"`
	CouponMoney     string `json:"couponmoney"`
	CommissionShare string `json:"commissionshare"`
	Commission      string `json:"commission"`
	CouponSurplus   string `json:"couponSurplus"`
	CouponNum       string `json:"couponnum"`
	CouponStartTime string `json:"couponstarttime"`
	CouponEndTime   string `json:"couponendtime"`
}

type PddHdk struct {
	Code  int          `json:"code"`
	MinId int          `json:"min_id"`
	Msg   string       `json:"msg"`
	Data  []PddHdkData `json:"data"`
}
type PddHdkData struct {
	GoodsId          string `json:"goods_id"`
	GoodsSign        string `json:"goods_sign"`
	GoodsName        string `json:"goodsname"`
	GoodsDesc        string `json:"goodsdesc"`
	ItemPrice        string `json:"itemprice"`
	ItemSale         string `json:"itemsale"`
	ItemSale2        string `json:"itemsale2"`
	TodaySale        string `json:"todaysale"`
	ItemPic          string `json:"itempic"`
	PddImage         string `json:"pdd_image"`
	CatId            string `json:"cat_id"`
	ItemEndPrice     string `json:"itemendprice"`
	CouponMoney      string `json:"couponmoney"`
	PromotionRate    string `json:"promotion_rate"`
	Commission       string `json:"commission"` // 预计可得（宝贝价格 * 佣金比例 / 100）
	CouponSurplus    string `json:"couponsurplus"`
	CouponNum        string `json:"couponnum"`
	CouponStartTime  string `json:"couponstarttime"`
	CouponEndTime    string `json:"couponendtime"`
	ExtraCouponMoney string `json:"extracouponmoney"`
}
