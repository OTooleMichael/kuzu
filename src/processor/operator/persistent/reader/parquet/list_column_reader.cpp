#include "processor/operator/persistent/reader/parquet/list_column_reader.h"

namespace kuzu {
namespace processor {

ListColumnReader::ListColumnReader(ParquetReader& reader,
    std::unique_ptr<common::LogicalType> type_p,
    const kuzu_parquet::format::SchemaElement& schema_p, uint64_t schema_idx_p,
    uint64_t max_define_p, uint64_t max_repeat_p,
    std::unique_ptr<ColumnReader> child_column_reader_p)
    : ColumnReader(reader, std::move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p),
      child_column_reader(std::move(child_column_reader_p)), overflow_child_count(0) {

    child_defines.resize(common::DEFAULT_VECTOR_CAPACITY);
    child_repeats.resize(common::DEFAULT_VECTOR_CAPACITY);
    child_defines_ptr = (uint8_t*)child_defines.ptr;
    child_repeats_ptr = (uint8_t*)child_repeats.ptr;

    child_filter.set();
}

void ListColumnReader::ApplyPendingSkips(uint64_t num_values) {
    pending_skips -= num_values;

    auto define_out = std::unique_ptr<uint8_t[]>(new uint8_t[num_values]);
    auto repeat_out = std::unique_ptr<uint8_t[]>(new uint8_t[num_values]);

    uint64_t remaining = num_values;
    uint64_t read = 0;

    while (remaining) {
        auto result_out = std::make_unique<common::ValueVector>(*type);
        parquet_filter_t filter;
        auto to_read = std::min<uint64_t>(remaining, common::DEFAULT_VECTOR_CAPACITY);
        read += Read(to_read, filter, define_out.get(), repeat_out.get(), result_out.get());
        remaining -= to_read;
    }

    if (read != num_values) {
        throw common::CopyException("Not all skips done!");
    }
}

uint64_t ListColumnReader::Read(uint64_t num_values, parquet_filter_t& filter, uint8_t* define_out,
    uint8_t* repeat_out, common::ValueVector* result_out) {
    common::offset_t result_offset = 0;
    auto result_ptr = reinterpret_cast<common::list_entry_t*>(result_out->getData());

    if (pending_skips > 0) {
        ApplyPendingSkips(pending_skips);
    }

    // if an individual list is longer than STANDARD_VECTOR_SIZE we actually have to loop the child
    // read to fill it
    bool finished = false;
    while (!finished) {
        uint64_t child_actual_num_values = 0;

        // check if we have any overflow from a previous read
        if (overflow_child_count == 0) {
            // we don't: read elements from the child reader
            child_defines.zero();
            child_repeats.zero();
            // we don't know in advance how many values to read because of the beautiful
            // repetition/definition setup we just read (up to) a vector from the child column, and
            // see if we have read enough if we have not read enough, we read another vector if we
            // have read enough, we leave any unhandled elements in the overflow vector for a
            // subsequent read
            auto child_req_num_values = std::min<uint64_t>(
                common::DEFAULT_VECTOR_CAPACITY, child_column_reader->GroupRowsAvailable());
            child_actual_num_values =
                child_column_reader->Read(child_req_num_values, child_filter, child_defines_ptr,
                    child_repeats_ptr, common::ListVector::getDataVector(result_out));
        } else {
            // we do: use the overflow values
            child_actual_num_values = overflow_child_count;
            overflow_child_count = 0;
        }

        if (child_actual_num_values == 0) {
            // no more elements available: we are done
            break;
        }
        auto current_chunk_offset = common::ListVector::getDataVectorSize(result_out);

        // hard-won piece of code this, modify at your own risk
        // the intuition is that we have to only collapse values into lists that are repeated *on
        // this level* the rest is pretty much handed up as-is as a single-valued list or NULL
        uint64_t child_idx;
        for (child_idx = 0; child_idx < child_actual_num_values; child_idx++) {
            if (child_repeats_ptr[child_idx] == max_repeat) {
                // value repeats on this level, append
                assert(result_offset > 0);
                result_ptr[result_offset - 1].size++;
                continue;
            }

            if (result_offset >= num_values) {
                // we ran out of output space
                finished = true;
                break;
            }
            if (child_defines_ptr[child_idx] >= max_define) {
                // value has been defined down the stack, hence its NOT NULL
                result_ptr[result_offset].offset = child_idx + current_chunk_offset;
                result_ptr[result_offset].size = 1;
            } else if (child_defines_ptr[child_idx] == max_define - 1) {
                // empty list
                result_ptr[result_offset].offset = child_idx + current_chunk_offset;
                result_ptr[result_offset].size = 0;
            } else {
                // value is NULL somewhere up the stack
                result_out->setNull(result_offset, true);
                result_ptr[result_offset].offset = 0;
                result_ptr[result_offset].size = 0;
            }

            repeat_out[result_offset] = child_repeats_ptr[child_idx];
            define_out[result_offset] = child_defines_ptr[child_idx];

            result_offset++;
        }
        if (child_idx < child_actual_num_values && result_offset == num_values) {
            overflow_child_count = child_actual_num_values - child_idx;
            // move values in the child repeats and defines *backward* by child_idx
            for (auto repdef_idx = 0; repdef_idx < overflow_child_count; repdef_idx++) {
                child_defines_ptr[repdef_idx] = child_defines_ptr[child_idx + repdef_idx];
                child_repeats_ptr[repdef_idx] = child_repeats_ptr[child_idx + repdef_idx];
            }
        }
    }
    return result_offset;
}

} // namespace processor
} // namespace kuzu
