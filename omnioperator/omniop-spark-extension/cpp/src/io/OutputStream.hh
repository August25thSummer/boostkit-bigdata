/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SPARK_OUTPUTSTREAM_HH
#define SPARK_OUTPUTSTREAM_HH

#include "SparkFile.hh"
#include "MemoryPool.hh"
#include "wrap/zero_copy_stream_wrapper.h"

namespace spark {

  /**
   * A subclass of Google's ZeroCopyOutputStream that supports output to memory
   * buffer, and flushing to OutputStream.
   * By extending Google's class, we get the ability to pass it directly
   * to the protobuf writers.
   */
  class BufferedOutputStream: public google::protobuf::io::ZeroCopyOutputStream {
  private:
    OutputStream * outputStream;
    std::unique_ptr<DataBuffer<char> > dataBuffer;
    uint64_t blockSize;

  public:
    BufferedOutputStream(MemoryPool& pool,
                      OutputStream * outStream,
                      uint64_t capacity,
                      uint64_t block_size);
    virtual ~BufferedOutputStream() override;

    virtual bool Next(void** data, int*size) override;
    virtual void BackUp(int count) override;
    virtual google::protobuf::int64 ByteCount() const override;
    virtual bool WriteAliasedRaw(const void * data, int size) override;
    virtual bool AllowsAliasing() const override;

    virtual std::string getName() const;
    virtual uint64_t getSize() const;
    virtual uint64_t flush();
    virtual bool NextNBytes(void** data, int size);

    virtual bool isCompressed() const { return false; }
  };

}

#endif // SPARK_OUTPUTSTREAM_HH
