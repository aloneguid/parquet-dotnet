namespace Parquet.Encodings {
    using System;

    static partial class RleBitpackedHybridEncoder {
            private static void Encode0(byte[] s, ref int consumed, Span<int> data) {

            //chunk identical values and write
            int lastValue = 0;
            int chunkCount = 0;
            for(int i = 0; i < data.Length; i++) {
                int item = data[i];

                if(chunkCount == 0) {
                    chunkCount = 1;
                    lastValue = item;
                } else if(item != lastValue || chunkCount == MaxValueCount) {
                    WriteRle0(s, ref consumed, chunkCount, lastValue);

                    chunkCount = 1;
                    lastValue = item;
                } else
                    chunkCount += 1;
            }

            if(chunkCount > 0)
                WriteRle0(s, ref consumed, chunkCount, lastValue);
        }

            private static void Encode1(byte[] s, ref int consumed, Span<int> data) {

            //chunk identical values and write
            int lastValue = 0;
            int chunkCount = 0;
            for(int i = 0; i < data.Length; i++) {
                int item = data[i];

                if(chunkCount == 0) {
                    chunkCount = 1;
                    lastValue = item;
                } else if(item != lastValue || chunkCount == MaxValueCount) {
                    WriteRle1(s, ref consumed, chunkCount, lastValue);

                    chunkCount = 1;
                    lastValue = item;
                } else
                    chunkCount += 1;
            }

            if(chunkCount > 0)
                WriteRle1(s, ref consumed, chunkCount, lastValue);
        }

            private static void Encode2(byte[] s, ref int consumed, Span<int> data) {

            //chunk identical values and write
            int lastValue = 0;
            int chunkCount = 0;
            for(int i = 0; i < data.Length; i++) {
                int item = data[i];

                if(chunkCount == 0) {
                    chunkCount = 1;
                    lastValue = item;
                } else if(item != lastValue || chunkCount == MaxValueCount) {
                    WriteRle2(s, ref consumed, chunkCount, lastValue);

                    chunkCount = 1;
                    lastValue = item;
                } else
                    chunkCount += 1;
            }

            if(chunkCount > 0)
                WriteRle2(s, ref consumed, chunkCount, lastValue);
        }

            private static void Encode3(byte[] s, ref int consumed, Span<int> data) {

            //chunk identical values and write
            int lastValue = 0;
            int chunkCount = 0;
            for(int i = 0; i < data.Length; i++) {
                int item = data[i];

                if(chunkCount == 0) {
                    chunkCount = 1;
                    lastValue = item;
                } else if(item != lastValue || chunkCount == MaxValueCount) {
                    WriteRle3(s, ref consumed, chunkCount, lastValue);

                    chunkCount = 1;
                    lastValue = item;
                } else
                    chunkCount += 1;
            }

            if(chunkCount > 0)
                WriteRle3(s, ref consumed, chunkCount, lastValue);
        }

            private static void Encode4(byte[] s, ref int consumed, Span<int> data) {

            //chunk identical values and write
            int lastValue = 0;
            int chunkCount = 0;
            for(int i = 0; i < data.Length; i++) {
                int item = data[i];

                if(chunkCount == 0) {
                    chunkCount = 1;
                    lastValue = item;
                } else if(item != lastValue || chunkCount == MaxValueCount) {
                    WriteRle4(s, ref consumed, chunkCount, lastValue);

                    chunkCount = 1;
                    lastValue = item;
                } else
                    chunkCount += 1;
            }

            if(chunkCount > 0)
                WriteRle4(s, ref consumed, chunkCount, lastValue);
        }

        }
}