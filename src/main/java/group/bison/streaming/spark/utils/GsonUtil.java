package group.bison.streaming.spark.utils;

import com.google.gson.Gson;
import org.springframework.http.converter.json.GsonBuilderUtils;

/**
 * Created by BSONG on 2019/10/7.
 */
public class GsonUtil {

    private static final Gson GSON = GsonBuilderUtils.gsonBuilderWithBase64EncodedByteArrays().create();

    public static Gson getGson() {
        return GSON;
    }
}
