package patos.ao3

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class FanficContentMapper : Mapper<Any, Text, Text, Text>() {
    override fun map(key: Any?, value: Text?, context: Context?) {
        TODO("Debería tomar cada archivo del .tar y parsearlo")
    }
}
