// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package http

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/prometheus/common/model"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/curl"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/downsample"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/promql"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/trace"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb/victoriaMetricsInstance"
)

// 返回结构化数据
type PromData struct {
	dimensions map[string]bool
	Tables     []*TablesItem    `json:"series"`
	Status     *metadata.Status `json:"status,omitempty"`
}

// NewPromData
func NewPromData(dimensions []string) *PromData {
	dimensionsMap := make(map[string]bool)
	for _, dimension := range dimensions {
		dimensionsMap[dimension] = true
	}
	return &PromData{
		dimensions: dimensionsMap,
	}
}

// Fill
func (d *PromData) Fill(tables *promql.Tables) error {
	d.Tables = make([]*TablesItem, 0)
	for index, table := range tables.Tables {
		tableItem := new(TablesItem)
		tableItem.Name = fmt.Sprintf("_result%d", index)
		tableItem.MetricName = table.MetricName
		tableItem.Columns = make([]string, 0, len(table.Headers))
		tableItem.Types = make([]string, 0, len(table.Headers))
		tableItem.GroupKeys = table.GroupKeys
		tableItem.GroupValues = table.GroupValues
		keyMap := make(map[string]bool)
		for _, key := range table.GroupKeys {
			keyMap[key] = true
		}

		indexList := make([]int, 0, len(table.Headers))
		for index, header := range table.Headers {
			// 是key则不输出
			if _, ok := keyMap[header]; ok {
				continue
			}
			if len(d.dimensions) != 0 {
				if _, ok := d.dimensions[header]; !ok {
					continue
				}
			}
			// 记录需要返回的字段及其索引
			tableItem.Columns = append(tableItem.Columns, header)
			tableItem.Types = append(tableItem.Types, table.Types[index])
			indexList = append(indexList, index)
		}
		values := make([][]interface{}, 0)
		for _, data := range table.Data {
			value := make([]interface{}, len(indexList))
			for valueIndex, headerIndex := range indexList {
				value[valueIndex] = data[headerIndex]
			}

			values = append(values, value)
		}
		tableItem.Values = values
		d.Tables = append(d.Tables, tableItem)
	}
	return nil

}

// Downsample 对结果数据进行降采样
func (d *PromData) Downsample(factor float64) {
	for _, table := range d.Tables {
		points := downsample.Downsample(table.GetPromPoints(), factor)
		table.SetValuesByPoints(points)
	}
}

// getTime
func getTime(timestamp string) (time.Time, error) {
	timeNum, err := strconv.Atoi(timestamp)
	if err != nil {
		return time.Time{}, errors.New("parse time failed")
	}
	return time.Unix(int64(timeNum), 0), nil
}

func directlyQuery(
	ctx context.Context, query *structured.CombinedQueryParams, spaceUid string,
) (*PromData, error) {
	ctx, span := trace.IntoContext(ctx, trace.TracerName, "directly-query")
	if span != nil {
		defer span.End()
	}

	info, err := getTimeInfo(query)
	if err != nil {
		return nil, err
	}

	queries := metadata.GetQueries(ctx)

	trace.InsertStringIntoSpan("queries-data", queries.String(), span)

	var instance tsdb.Instance
	storage, err := tsdb.GetStorage(queries.DirectlyClusterID())
	if err != nil {
		return nil, err
	}
	if storage.Instance != nil {
		instance = storage.Instance
	} else {
		curl := &curl.HttpCurl{Log: log.OtLogger}
		address := fmt.Sprintf("%s/%s", storage.Address, storage.UriPath)
		instance = victoriaMetricsInstance.NewInstance(ctx, address, storage.Timeout, curl)
	}

	promExpr, err := query.ToProm(ctx, &structured.Option{
		IsOnlyParse: true,
		SpaceUid:    spaceUid,
	})
	if err != nil {
		return nil, err
	}
	oldStmt := promExpr.GetExpr().String()
	trace.InsertStringIntoSpan("old-stmt", oldStmt, span)
	sp := structured.NewStructParser(oldStmt)
	_, err = sp.ParseNew()
	if err != nil {
		return nil, err
	}

	// 还原 count 计算方法
	for _, rn := range queries.GetIsCountReferenceNameList() {
		sp.ToggleCountAndSum(rn, structured.SumOverTime)
	}

	// 更新 metricName
	sp.UpdateMetricName(queries.DirectlyMetricName(), queries.DirectlyLabelsMatcher())
	stmt := sp.String()

	trace.InsertStringIntoSpan("stmt", stmt, span)
	trace.InsertStringIntoSpan("start", info.Start.String(), span)
	trace.InsertStringIntoSpan("end", info.Stop.String(), span)
	trace.InsertStringIntoSpan("step", info.Interval.String(), span)

	log.Infof(ctx, "directly query old promql: %s", oldStmt)
	log.Infof(ctx, "directly query new promql: %s", stmt)

	metadata.SetExpand(ctx, &metadata.VmExpand{ResultTableGroup: queries.DirectlyResultTable()})

	res, err := instance.QueryRange(ctx, stmt, info.Start, info.Stop, info.Interval)
	if err != nil {
		return nil, err
	}

	seriesNum := 0
	pointsNum := 0

	tables := promql.NewTables()
	for index, series := range res {
		tables.Add(promql.NewTable(index, series))

		seriesNum++
		pointsNum += len(series.Points)
	}
	resp := NewPromData(query.ResultColumns)
	err = resp.Fill(tables)
	if err != nil {
		return nil, err
	}

	trace.InsertIntIntoSpan("resp-series-num", seriesNum, span)
	trace.InsertIntIntoSpan("resp-points-num", pointsNum, span)

	return resp, nil
	return &PromData{}, nil
}

// HandleRawPromQuery
func HandleRawPromQuery(ctx context.Context, stmt string, query *structured.CombinedQueryParams) (*PromData, error) {
	info, err := getTimeInfo(query)
	if err != nil {
		return nil, err
	}

	// 调用模块查询结果
	tables, err := promql.QueryRange(ctx, stmt, info.Start, info.Stop, info.Interval)
	if err != nil {
		log.Errorf(ctx, "query prom sql failed for->[%s]", err)
		return nil, err
	}
	log.Debugf(context.TODO(), "query prom:%s success", stmt)

	// 将结果进行格式转换
	resp := NewPromData(query.ResultColumns)
	err = resp.Fill(tables)

	if err != nil {
		log.Errorf(ctx, "fill prom result failed for->[%s]", err)
		return nil, err
	}
	return resp, nil
}

// TimeInfo
type TimeInfo struct {
	Start    time.Time
	Stop     time.Time
	Interval time.Duration
}

// getTimeInfo
func getTimeInfo(query *structured.CombinedQueryParams) (*TimeInfo, error) {
	var start time.Time
	var stop time.Time
	var interval time.Duration
	var dTmp model.Duration
	var err error
	info := new(TimeInfo)
	if query.Start == "" {
		return nil, errors.New("start time cannot be empty")
	}
	start, err = getTime(query.Start)

	if err != nil {
		return nil, err
	}
	if query.End == "" {
		stop = time.Now()
	} else {
		stop, err = getTime(query.End)
		if err != nil {
			return nil, err
		}
	}

	if query.Step == "" {
		interval = promql.GetDefaultStep()
	} else {
		dTmp, err = model.ParseDuration(query.Step)
		interval = time.Duration(dTmp)
		if err != nil {
			return nil, err
		}
	}

	// start需要根据step对齐
	start = time.Unix(int64(math.Floor(float64(start.Unix())/interval.Seconds())*interval.Seconds()), 0)

	info.Start = start
	info.Stop = stop
	info.Interval = interval
	return info, nil
}
