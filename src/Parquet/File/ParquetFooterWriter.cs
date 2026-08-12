using System;
using System.IO;
using Parquet.Encryption;
using Parquet.Extensions;
using Parquet.Meta;
using Parquet.Meta.Proto;

namespace Parquet.File;

internal static class ParquetFooterWriter {
    public static byte[] Serialize(ThriftFooter footer, ParquetFileCryptoContext? cryptoContext) {
        ArgumentNullException.ThrowIfNull(footer);

        byte[] footerBytes = footer.Serialize();
        using var output = new MemoryStream();

        if(cryptoContext == null) {
            output.Write(footerBytes);
        } else if(cryptoContext.EncryptedFooter) {
            var cryptoMetadata = new FileCryptoMetaData {
                EncryptionAlgorithm = cryptoContext.ThriftAlgorithm,
                KeyMetadata = cryptoContext.FooterKeyMetadata
            };
            cryptoMetadata.Write(new ThriftCompactProtocolWriter(output));
            output.Write(cryptoContext.Footer.Encrypt(footerBytes, ParquetModuleType.Footer));
        } else {
            output.Write(footerBytes);
            output.Write(cryptoContext.Footer.SignFooter(footerBytes));
        }

        int footerLength = checked((int)output.Length);
        output.WriteInt32(footerLength);
        output.Write(cryptoContext is { EncryptedFooter: true }
            ? ParquetActor.EncryptedMagicBytes
            : ParquetActor.MagicBytes);
        return output.ToArray();
    }
}
