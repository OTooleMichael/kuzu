add_library(kuzu_processor_operator_persistent_reader
        OBJECT
        column_reader.cpp
        csv_reader.cpp
        rdf_reader.cpp
        struct_column_reader.cpp
        parquet_reader.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_persistent_reader>
        PARENT_SCOPE)
