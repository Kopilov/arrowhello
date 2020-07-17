#include <iostream>
#include "arrow/api.h"
#include "arrow/memory_pool.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"


int writeIntArray() {
    std::cout << "writeIntArray!" << std::endl;
    arrow::Int64Builder builder;
    // Make place for 8 values in total
    builder.Resize(8);
    // Bulk append the given values (with a null in 4th place as indicated by the
    // validity vector)
    std::vector<bool> validity = {true, true, true, false, true, true, true, true};
    std::vector<int64_t> values = {1, 2, 3, 0, 5, 6, 7, 8};
    builder.AppendValues(values, validity);

    std::shared_ptr<arrow::Array> array;
    arrow::Status st = builder.Finish(&array);

    int64_t l = (*array).length();
    std::shared_ptr<arrow::Int64Array> int64_array = std::static_pointer_cast<arrow::Int64Array>(array);
    std::cout << (*int64_array).Value(0) << std::endl;
    std::cout << int64_array->Value(1) << std::endl;
    std::cout << int64_array->Value(2) << std::endl;
    std::cout << int64_array->Value(3) << std::endl;
    std::cout << int64_array->Value(4) << std::endl;
    std::cout << int64_array->Value(5) << std::endl;
    std::cout << int64_array->Value(6) << std::endl;
    std::cout << int64_array->Value(7) << std::endl;
    int64_t s = sizeof(int64_array->raw_values());
    std::cout << st << " " << s << " " << l << std::endl;

    std::shared_ptr<arrow::Field> fieldExample = field("col1", arrow::int64());
    std::shared_ptr<arrow::Schema> schemaExample = arrow::schema({fieldExample});
    std::shared_ptr<arrow::Table> tableExample = arrow::Table::Make(schemaExample, {array});

    std::shared_ptr<arrow::io::FileOutputStream> openedFile = *(arrow::io::FileOutputStream::Open("test"));
    arrow::ipc::feather::WriteTable(*tableExample, &(*openedFile));
    return 0;
}


int readLongAray() {
    std::cout << "readLongAray!" << std::endl;
    //arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> input = arrow::io::ReadableFile::Open("test");
    std::shared_ptr<arrow::ipc::feather::Reader> reader = (*arrow::ipc::feather::Reader::Open(
                                                               *(arrow::io::MemoryMappedFile::Open("test", arrow::io::FileMode::READWRITE)))
                                                           );
    std::shared_ptr<arrow::Table> tableTarget;
    reader->Read(&tableTarget);
    std::shared_ptr<arrow::Array> array = tableTarget->column(0)->chunk(0);
    std::shared_ptr<arrow::Int64Array> int64_array = std::static_pointer_cast<arrow::Int64Array>(array);
    for (int i = 0; i < int64_array->length(); i++) {
        std::cout << int64_array->Value(i) << std::endl;
    }
    return 0;
}

int readIntMatrix() {
    std::cout << "readIntMatrix!" << std::endl;
    //arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> input = arrow::io::ReadableFile::Open("test");
    std::shared_ptr<arrow::ipc::feather::Reader> reader = (*arrow::ipc::feather::Reader::Open(
                                                               *(arrow::io::MemoryMappedFile::Open("testList", arrow::io::FileMode::READWRITE)))
                                                           );
    std::shared_ptr<arrow::Table> tableTarget;
    reader->Read(&tableTarget);
    std::shared_ptr<arrow::Array> array = tableTarget->column(0)->chunk(0);
    std::cout << (*array).type()->name() << std::endl;
    std::shared_ptr<arrow::FixedSizeListArray> list_array = std::static_pointer_cast<arrow::FixedSizeListArray>(array);
    std::cout << "size " << list_array->length() << std::endl;

    for (int i = 0; i < list_array->length(); i++) {
        list_array->value_slice(i);
        std::shared_ptr<arrow::Int32Array> list = std::static_pointer_cast<arrow::Int32Array>(list_array->value_slice(i));
        for (int j = 0; j < list->length(); j++) {
            std::cout << list->Value(j) << "\t";
        }
        std::cout << std::endl;
    }
    return 0;
}

int readStringMatrix() {
    std::cout << "readIntMatrix!" << std::endl;
    //arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> input = arrow::io::ReadableFile::Open("test");
    std::shared_ptr<arrow::ipc::feather::Reader> reader = (*arrow::ipc::feather::Reader::Open(
                                                               *(arrow::io::MemoryMappedFile::Open("testList", arrow::io::FileMode::READWRITE)))
                                                           );
    std::shared_ptr<arrow::Table> tableTarget;
    reader->Read(&tableTarget);
    std::shared_ptr<arrow::Array> array = tableTarget->column(0)->chunk(0);
    std::shared_ptr<arrow::FixedSizeListArray> list_array = std::static_pointer_cast<arrow::FixedSizeListArray>(array);
    std::cout << "size " << list_array->length() << std::endl;

    for (int i = 0; i < list_array->length(); i++) {


        std::shared_ptr<arrow::Array> list1 = list_array->value_slice(i);


        std::shared_ptr<arrow::StringArray> list = std::static_pointer_cast<arrow::StringArray>(list_array->value_slice(i));
        for (int j = 0; j < list->length(); j++) {
            std::cout << list->GetView(j) << "\t";
        }
        std::cout << std::endl;
    }
    return 0;
}

int writeIntMatrix() {
    std::cout << "writeIntMatrix!" << std::endl;
    std::shared_ptr<arrow::Int32Builder> intBuilder = std::make_shared<arrow::Int32Builder>();
    std::shared_ptr<arrow::FixedSizeListBuilder> builder = std::make_shared<arrow::FixedSizeListBuilder>(arrow::default_memory_pool(), intBuilder, 10);
    builder->Resize(20);
    for (int i = 1; i <= 20; i++) {
        builder->Append();
        for (int j = 1; j <= 10; j++) {
            intBuilder->Append(i * j);
        }
    }
    std::shared_ptr<arrow::FixedSizeListArray> array;
    builder->Finish(&array);

    std::shared_ptr<arrow::Field> fieldExample = field("testList", arrow::fixed_size_list(arrow::int32(), 10));
    std::shared_ptr<arrow::Schema> schemaExample = arrow::schema({fieldExample});
    std::shared_ptr<arrow::Table> tableExample = arrow::Table::Make(schemaExample, {array});

    std::shared_ptr<arrow::io::FileOutputStream> openedFile = *(arrow::io::FileOutputStream::Open("testList"));
    arrow::ipc::feather::WriteTable(*tableExample, &(*openedFile));
}

int writeUnionMatrix() {
    std::cout << "writeUnionMatrix!" << std::endl;
    std::shared_ptr<arrow::Int32Builder> intBuilder = std::make_shared<arrow::Int32Builder>();
    std::shared_ptr<arrow::StringBuilder> stringBuilder = std::make_shared<arrow::StringBuilder>();
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> children = {intBuilder, stringBuilder};
    std::cout << "SparseUnionBuilder" << std::endl;
    std::vector<std::shared_ptr<arrow::Field>> fields = {std::make_shared<arrow::Field>("", arrow::int32()), std::make_shared<arrow::Field>("", arrow::utf8())};
    std::vector<int8_t> type_codes = {1, 2};
    std::shared_ptr<arrow::DataType> type = std::make_shared<arrow::UnionType>(fields, type_codes);
    std::shared_ptr<arrow::SparseUnionBuilder> unionBuilder = std::make_shared<arrow::SparseUnionBuilder>(arrow::default_memory_pool(), children, type);
    std::cout << "SparseUnionBuilder2" << std::endl;
    std::shared_ptr<arrow::ListBuilder> builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), unionBuilder);

    builder->Resize(20);

    for (int i = 1; i <= 20; i++) {
        builder->Append();
        for (int j = 1; j <= 10; j++) {
            if (j % 2 == 0) {
                intBuilder->Append(i * j);
            } else {
                char str[80];
                sprintf (str, "%d * %d = %d", i, j, i*j);
                stringBuilder->Append( std::string(str));
            }
        }
    }

    std::shared_ptr<arrow::Array> array;
    builder->Finish(&array);

    std::shared_ptr<arrow::Field> fieldExample = field("testList", arrow::list(type));
    std::shared_ptr<arrow::Schema> schemaExample = arrow::schema({fieldExample});
    std::shared_ptr<arrow::Table> tableExample = arrow::Table::Make(schemaExample, {array});
    std::shared_ptr<arrow::io::FileOutputStream> openedFile = *(arrow::io::FileOutputStream::Open("testList"));
    arrow::ipc::feather::WriteTable(*tableExample, &(*openedFile));
}

int readUnionMatrix() {
    std::cout << "readUnionMatrix!" << std::endl;
    std::shared_ptr<arrow::ipc::feather::Reader> reader = (*arrow::ipc::feather::Reader::Open(
                                                               *(arrow::io::MemoryMappedFile::Open("testList", arrow::io::FileMode::READWRITE)))
                                                           );
    std::shared_ptr<arrow::Table> tableTarget;
    reader->Read(&tableTarget);
    std::cout << "afterRead" << std::endl;
    std::shared_ptr<arrow::ChunkedArray> carray = tableTarget->column(0);
    std::cout << "gotCArray" << std::endl;
    std::shared_ptr<arrow::Array> array = carray->chunk(0);
    std::cout << "gotArray" << std::endl;
    std::cout << (*array).type()->name() << std::endl;
    std::shared_ptr<arrow::FixedSizeListArray> list_array = std::static_pointer_cast<arrow::FixedSizeListArray>(array);
    std::cout << "size " << list_array->length() << std::endl;

    for (int i = 0; i < list_array->length(); i++) {
        std::cout << "i = " << i << std::endl;

        std::shared_ptr<arrow::Array> list_ = list_array->value_slice(i);
        std::cout << "slice" << std::endl;
        std::cout << typeid(*list_).name() << std::endl;
        std::shared_ptr<arrow::UnionArray> list = std::static_pointer_cast<arrow::UnionArray>(list_);
        std::cout << "Got as UnionArray" << std::endl;

        std::cout << list->length() << std::endl;
        for (int j = 0; j < list->length(); j++) {
//            std::cout << list->Value(j) << "\t";
        }
        std::cout << std::endl;
    }
}

int main() {
    readUnionMatrix();
}
