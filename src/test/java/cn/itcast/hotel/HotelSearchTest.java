package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.tdunning.math.stats.Sort;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.*;

public class HotelSearchTest {

    private RestHighLevelClient client;

    @Autowired
    private IHotelService hotelService;

    @Test
    void testMatchAll() throws IOException {
        SearchRequest request = new SearchRequest("hotel");

        request.source().query(QueryBuilders.matchQuery("all","如家"));

//        request.source().query(QueryBuilders.matchQuery("all","如家"));

//        request.source().query(QueryBuilders.termQuery("city","杭州"));

//        request.source().query(QueryBuilders.rangeQuery("price").gte(100).lte(150));

//        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
//        boolQuery.must(QueryBuilders.termQuery("city","杭州"));
//        boolQuery.filter(QueryBuilders.rangeQuery("price").gt(100));

//        int page = 2; int size = 5;
//        request.source().query(QueryBuilders.matchAllQuery());
//        request.source().sort("price", SortOrder.ASC);
//        request.source().from((page - 1) * size).size(5);

        request.source().highlighter(new HighlightBuilder()
                .field("name")
                .requireFieldMatch(false)
        );

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        Map<String, List<String>> filters = filters();
        System.out.println(filters);
        //解析响应
        handleResponse(response);

    }


    Map<String, List<String>> filters() throws IOException {
        Map<String, List<String>> map = new HashMap<>();

        SearchRequest request = new SearchRequest("hotel");

        request.source().size(0);

        request.source().aggregation(AggregationBuilders
                .terms("cityAgg")
                .field("city")
                .size(10));

        request.source().aggregation(AggregationBuilders
                .terms("starAgg")
                .field("starName")
                .size(10));

        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(10));

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //解析聚合
        Aggregations aggregations = response.getAggregations();

        Map<String, Aggregation> asMap = aggregations.getAsMap();
        for (String key : asMap.keySet()) {
            Terms terms = aggregations.get(key);
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            ArrayList<String> names = new ArrayList<>();
            for (Terms.Bucket bucket : buckets) {
                names.add(bucket.getKeyAsString());
            }
            map.put(key, names);
        }
        return map;

    }

    public static void handleResponse(SearchResponse response) {
        SearchHits searchHits = response.getHits();

        long total = searchHits.getTotalHits().value;
        System.out.println("共搜索到多少条数据:"+total);
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            if (hotelDoc.getId() ==  null) {
                continue;
            }

            //解析高亮
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (CollectionUtils.isEmpty(highlightFields)) {
                continue;
            }
            HighlightField highlightField = highlightFields.get("name");

            if (highlightField == null) {
                continue;
            }
            String name = highlightField.getFragments()[0].string();
            hotelDoc.setName(name);
        }
    }

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.159.128:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        client.close();
    }
}
