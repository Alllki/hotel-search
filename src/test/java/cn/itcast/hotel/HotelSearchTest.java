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
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
        //解析响应
        handleResponse(response);

    }

    @Test
    void testAggregation() throws IOException {
        SearchRequest request = new SearchRequest("hotel");

        request.source().size(0);

        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(10));

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        Aggregations aggregations = response.getAggregations();
        Terms brandTerms = aggregations.get("brandAgg");

        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            System.out.println(key);
        }
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
            System.out.println(hotelDoc);
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
