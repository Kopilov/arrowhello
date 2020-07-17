import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.FixedSizeListVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListWriter
import org.apache.arrow.vector.holders.VarCharHolder
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.UnionMode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import java.io.File
import java.nio.channels.FileChannel
import java.nio.charset.Charset
import java.nio.file.StandardOpenOption

fun writeLongVector() {
    val fieldVector = BigIntVector("f1", RootAllocator())
    val s = 1000
    fieldVector.allocateNew()
    fieldVector.valueCount = s
    for (i in 1..s) {
        fieldVector[i-1] = i * 3L
    }
    val table = VectorSchemaRoot(arrayListOf(fieldVector))
    ArrowFileWriter(table, null, openFileChannel(File("test"))).use { writer ->
        writer.writeBatch()
    }
}

fun writeListIntVector() {
    val listVector = FixedSizeListVector.empty("testList", 10, RootAllocator())
    listVector.writer
    val vectorWriter = listVector.writer
    vectorWriter.allocate()
    vectorWriter.setValueCount(20)
    for (i in 1..20) {
        vectorWriter.startList()
        for (j in 1..10) {
            vectorWriter.integer().writeInt(i * j)
        }
        vectorWriter.endList()
    }
    val table = VectorSchemaRoot(arrayListOf(listVector))
    ArrowFileWriter(table, null, openFileChannel(File("testList"))).use { writer ->
        writer.writeBatch()
    }
}

fun writeListStringVector() {
    val listVector = FixedSizeListVector.empty("testList", 10, RootAllocator())
    listVector.writer
    val vectorWriter = listVector.writer
    vectorWriter.allocate()
    vectorWriter.setValueCount(20)
    for (i in 1..20) {
        vectorWriter.startList()
        for (j in 1..10) {
            val h = VarCharHolder()
            val stringExample = "$i * $j = ${i * j}\t"
            val byteArray = stringExample.toByteArray(Charset.forName("UTF-8"))
            h.start = 0
            h.end = byteArray.size - 1
            h.buffer = listVector.allocator.buffer(byteArray.size.toLong())
            h.buffer.setBytes(0, byteArray)
            vectorWriter.varChar().write(h)
        }
        vectorWriter.endList()
    }
    val table = VectorSchemaRoot(arrayListOf(listVector))
    ArrowFileWriter(table, null, openFileChannel(File("testList"))).use { writer ->
        writer.writeBatch()
    }
}


fun writeUnionListVector() {
    val listVector = ListVector.empty("testList", RootAllocator())
    listVector.writer
    val vectorWriter = listVector.writer
    vectorWriter.allocate()
    vectorWriter.setValueCount(20)
    for (i in 1..20) {
        vectorWriter.startList()
        for (j in 1..10) {
            if (j % 2 == 0) {
                vectorWriter.integer().writeInt(i * j)
            } else {
                val h = VarCharHolder()
                val stringExample = "$i * $j = ${i * j}\t"
                val byteArray = stringExample.toByteArray(Charset.forName("UTF-8"))
                h.start = 0
                h.end = byteArray.size - 1
                h.buffer = listVector.allocator.buffer(byteArray.size.toLong())
                h.buffer.setBytes(0, byteArray)
                vectorWriter.varChar().write(h)
            }
        }
        vectorWriter.endList()
    }
    val table = VectorSchemaRoot(arrayListOf(listVector))
    ArrowFileWriter(table, null, openFileChannel(File("testList"))).use { writer ->
        writer.writeBatch()
    }
}

fun writeUnionListVectorBySchema() {

//    val listVector = FixedSizeListVector.empty("testList", 10, RootAllocator())

//    println(listVector.field.type.toString())
//    println(listVector.field.children[0].type.toString())
//    listVector.field.children[0].fieldType.type = ArrowType.Union(UnionMode.Sparse, intArrayOf())
//    println(listVector.field.children[0].type.toString())
//    println(listVector.field.children[0].children[0].type.toString())
//    println(listVector.field.children[0].children[1].type.toString())


    val field = Field(
        "testList",
        FieldType(true, ArrowType.FixedSizeList(10), null),
        arrayListOf(
            Field("testList", FieldType(true, ArrowType.Union(UnionMode.Sparse, intArrayOf()), null), arrayListOf(
//                Field("testList", FieldType(true, ArrowType.Int(32, true), null), arrayListOf()),
//                Field("testList", FieldType(true, ArrowType.Utf8(), null), arrayListOf())
            ))
            //Field("testList", FieldType(true, ArrowType.FixedSizeList(10), null), arrayListOf())
        )
    )
    val schema = Schema(arrayListOf(field))
    val table = VectorSchemaRoot.create(schema, RootAllocator())// VectorSchemaRoot(arrayListOf(listVector))
    val listVector = table.getVector(0) as FixedSizeListVector

    val vectorWriter = listVector.writer
    vectorWriter.allocate()
    vectorWriter.setValueCount(20)
    for (i in 1..20) {
        vectorWriter.startList()
        for (j in 1..10) {
            if (i% 2 == 0) {
                vectorWriter.integer().writeInt(i * j)
            } else {
                val h = VarCharHolder()
                val stringExample = "$i * $j = ${i * j}\t"
                val byteArray = stringExample.toByteArray(Charset.forName("UTF-8"))
                h.start = 0
                h.end = byteArray.size - 1
                h.buffer = listVector.allocator.buffer(byteArray.size.toLong())
                h.buffer.setBytes(0, byteArray)
                vectorWriter.varChar().write(h)
            }
        }
        vectorWriter.endList()
    }
    vectorWriter.setValueCount(20)

    println(listVector.valueCount)
    //table.addVector(0, listVector)
    println(table.rowCount)
    ArrowFileWriter(table, null, openFileChannel(File("testList"))).use { writer ->
        writer.writeBatch()
    }
    println(table.schema.toJson())
}

fun readListVector() {
    val reader = ArrowFileReader(openFileChannel(File("testList")), RootAllocator())
    reader.initialize()
    println(reader.vectorSchemaRoot.schema.toJson())
    reader.loadNextBatch()
    val table = reader.vectorSchemaRoot

    val matrix = table.getVector(0) as FixedSizeListVector
    println (matrix.valueCount)
    println (matrix.listSize)

    for (i in 1..matrix.valueCount) {
        for (j in 1..matrix.listSize) {
            //println(matrix.listSize)
            print ((matrix.getObject(i-1) as List<*>)[j-1])
            //print (matrix.reader.readObject())
            print("\t")
        }
        println("")
    }
  //  }
}

fun readUnionListVector() {
    val reader = ArrowFileReader(openFileChannel(File("testList")), RootAllocator())
    println(reader.vectorSchemaRoot.schema.toJson())
    reader.initialize()
    reader.loadNextBatch()
    val table = reader.vectorSchemaRoot

    println(table.getVector(0).javaClass.canonicalName)
    val matrix = table.getVector(0) as ListVector
    println (matrix.valueCount)
//    println (matrix.listSize)

    for (i in 1..matrix.valueCount) {
        println(matrix.getObject(i-1).javaClass.canonicalName)
        val row = matrix.getObject(i-1) as List<*>
        println(row)
        for (j in 1..row.size) {
            //println(matrix.listSize)
            print (row[j-1])
            //print (matrix.reader.readObject())
            print("\t")
        }
        println("")
    }
    //  }
}
/**
 * Open java nio FileChannel to make File memory-mapped
 */
fun openFileChannel(memoryMappingFile: File): FileChannel {
    return FileChannel.open(
        memoryMappingFile.toPath(),
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE
    )
}

fun main(args: Array<String>) {
    writeUnionListVector()
    readUnionListVector()
}
