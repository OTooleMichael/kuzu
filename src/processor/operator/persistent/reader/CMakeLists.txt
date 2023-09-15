add_library(kuzu_processor_operator_persistent_reader
        OBJECT
        base_csv_reader.cpp
        npy_reader.cpp
        parallel_csv_reader.cpp
        serial_csv_reader.cpp
        rdf_reader.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_persistent_reader>
        PARENT_SCOPE)
