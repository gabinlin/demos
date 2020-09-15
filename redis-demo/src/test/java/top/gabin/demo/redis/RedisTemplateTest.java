package top.gabin.demo.redis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

@SpringBootTest
public class RedisTemplateTest {

    @Autowired
    private RedisTemplate redisTemplate;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void test() {
//        redisTemplate操作的key会有一个前缀
        ValueOperations valueOperations = stringRedisTemplate.opsForValue();
        System.out.println(stringRedisTemplate.getKeySerializer());
        valueOperations.set("name", "gabin");
    }

}
