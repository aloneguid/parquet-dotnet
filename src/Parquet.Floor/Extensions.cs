using System.Diagnostics;
using Parquet.Meta;

namespace Parquet.Floor {
    static class Extensions {
        public static string ToSimpleString(this LogicalType? lt) {
            if(lt == null)
                return string.Empty;

            if(lt.UUID != null)
                return "UUID";

            if(lt.STRING != null)
                return "STRING";

            if(lt.MAP != null)
                return "MAP";

            if(lt.LIST != null)
                return "LIST";

            if(lt.ENUM != null)
                return "ENUM";

            if(lt.DECIMAL != null)
                return $"DECIMAL (precision: {lt.DECIMAL.Precision}, scale: {lt.DECIMAL.Scale})";

            if(lt.DATE != null)
                return $"DATE";

            if(lt.TIME != null) {
                string unit = lt.TIME.Unit.MICROS != null
                    ? "MICROS"
                    : lt.TIME.Unit.MILLIS != null
                        ? "MILLIS"
                        : "NANOS";
                return $"TIME (unit: {unit}, isAdjustedToUTC: {lt.TIME.IsAdjustedToUTC})";
            }

            if(lt.TIMESTAMP != null) {
                string unit = lt.TIMESTAMP.Unit.MICROS != null
                    ? "MICROS"
                    : lt.TIMESTAMP.Unit.MILLIS != null
                        ? "MILLIS"
                        : "NANOS";
                return $"TIMESTAMP (unit: {unit}, isAdjustedToUTC: {lt.TIMESTAMP.IsAdjustedToUTC})";
            }

            if(lt.INTEGER != null)
                return $"INTEGER (bitWidth: {lt.INTEGER.BitWidth}, isSigned: {lt.INTEGER.IsSigned})";

            if(lt.UNKNOWN != null)
                return "UNKNOWN";

            if(lt.JSON != null)
                return "JSON";

            if(lt.BSON != null)
                return "BSON";

            if(lt.UUID != null)
                return "UUID";

            return "?";
        }

        public static void OpenInBrowser(this string url) {
            var p = new Process();
            p.StartInfo.UseShellExecute = true;
            p.StartInfo.FileName = url;
            p.Start();
        }
    }
}
