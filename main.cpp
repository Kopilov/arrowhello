#include <iostream>
#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"


int main1() {
    std::cout << "Hello Arrow!" << std::endl;
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


int main() {
    std::cout << "Hello Arrow2!" << std::endl;
    arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> input = arrow::io::ReadableFile::Open("test");
    std::shared_ptr<arrow::ipc::feather::Reader> reader = (*arrow::ipc::feather::Reader::Open(
                                                               *(arrow::io::MemoryMappedFile::Open("test", arrow::io::FileMode::READWRITE)))
                                                           );
    std::shared_ptr<arrow::Table> tableTarget;
    reader->Read(&tableTarget);
    std::shared_ptr<arrow::Array> array = tableTarget->column(0)->chunk(0);
    std::shared_ptr<arrow::Int64Array> int64_array = std::static_pointer_cast<arrow::Int64Array>(array);
    std::cout << (*int64_array).Value(0) << std::endl;
    std::cout << int64_array->Value(1) << std::endl;
    std::cout << int64_array->Value(2) << std::endl;
    std::cout << int64_array->Value(3) << std::endl;
    std::cout << int64_array->Value(4) << std::endl;
    std::cout << int64_array->Value(5) << std::endl;
    std::cout << int64_array->Value(6) << std::endl;
    std::cout << int64_array->Value(7) << std::endl;
    return 0;
}

