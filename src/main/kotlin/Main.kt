import patos.ao3.FilterTheatrical
import patos.ao3.SortWordCounts

private const val PREFIX = "org.mdp.hadoop.cli."

fun main(args: Array<String>) {
    try {
        if (args.isEmpty()) {
            val sb = StringBuffer()
            sb.append("missing <utility> arg where <utility> one of")
            sb.append("\n\t").append(FilterTheatrical::class.java.simpleName)
                .append(": Filter theatrical movies")
            sb.append("\n\t").append(SortWordCounts::class.java.simpleName)
                .append(": Sort words by count descending")

            usage(sb.toString())
        }


        val cls = Class.forName(PREFIX + args[0])

        val mainMethod = cls.getMethod("main", Array<String>::class.java)

        val mainArgs = arrayOfNulls<String>(args.size - 1)
        System.arraycopy(args, 1, mainArgs, 0, mainArgs.size)

        val time = System.currentTimeMillis()

        mainMethod.invoke(null, mainArgs)

        val time1 = System.currentTimeMillis()

        System.err.println("time elapsed " + (time1 - time) + " ms")
    } catch (e: Throwable) {
        e.printStackTrace()
        usage(e.toString())
    }
}

private fun usage(msg: String) {
    System.err.println(msg)
    System.exit(-1)
}