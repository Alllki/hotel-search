package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;
    @Override
    public PageResult search(RequestParams params) {
        try {
            //1. 准备Request
            SearchRequest request = new SearchRequest("hotel");
            buildBasicQuery(request,params);
            //分页
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page - 1) * size).size(size);

            //排序
            String location = params.getLocation();
            if (location != null && location.equals("")) {
                request.source().sort(SortBuilders.
                        geoDistanceSort("location", new GeoPoint(location))
                        .order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS)
                );
            }

            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //解析响应
            return handleResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void buildBasicQuery(SearchRequest request, RequestParams params) {
        //2. 准备DSL
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();

        //关键字搜索
        String key = params.getKey();
        if (StringUtils.isEmpty(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }

        //城市条件
        if (params.getCity() != null && !params.getCity().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }

        //品牌条件
        if (params.getBrand() != null && !params.getBrand().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }

        //星级条件
        if (params.getStarName() != null && !params.getStarName().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("star_name", params.getStarName()));
        }

        //价格范围
        if (params.getMaxPrice() != null && params.getMinPrice() != null) {
            boolQuery.filter(QueryBuilders.rangeQuery("price").
                    gte(params.getMinPrice()).lte(params.getMaxPrice()));
        }

        FunctionScoreQueryBuilder functionScoreQuery =
                QueryBuilders.functionScoreQuery(
                        boolQuery,
                        //function score数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                //其中的一个具体的function score元素
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        //过滤条件
                                        QueryBuilders.termQuery("isAD", true),
                                        //算分函数
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });
        request.source().query(functionScoreQuery);

    }

    public static PageResult handleResponse(SearchResponse response) {
        SearchHits searchHits = response.getHits();

        long total = searchHits.getTotalHits().value;
        SearchHit[] hits = searchHits.getHits();

        List<HotelDoc> hotelDocs = new ArrayList<>();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            if (hotelDoc.getId() ==  null) {
                continue;
            }

            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)) {
                HighlightField highlightField = highlightFields.get("name");

                if (highlightField != null) {
                    String name = highlightField.getFragments()[0].string();
                    hotelDoc.setName(name);
                }
            }

            Object[] sortValues = hit.getSortValues();
            if (sortValues.length > 0) {
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotelDocs.add(hotelDoc);
        }
        return new PageResult(total, hotelDocs);
    }
}
