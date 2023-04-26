<Query Kind="Program">
  <NuGetReference>Humanizer</NuGetReference>
  <Namespace>Humanizer</Namespace>
  <Namespace>System.Web</Namespace>
</Query>

#nullable enable

const string Spacing = "    ";
const string inputPath = @"C:\dev\parquet-dotnet\src\Parquet\Meta\parquet.xml";
const string outputDir = @"C:\dev\parquet-dotnet\src\Parquet\Meta\";

private static class Types {
    public const byte Stop = 0x00;
    public const byte BooleanTrue = 0x01;
    public const byte BooleanFalse = 0x02;
    public const byte Byte = 0x03;
    public const byte I16 = 0x04;
    public const byte I32 = 0x05;
    public const byte I64 = 0x06;
    public const byte Double = 0x07;
    public const byte Binary = 0x08;
    public const byte List = 0x09;
    public const byte Set = 0x0A;
    public const byte Map = 0x0B;
    public const byte Struct = 0x0C;
    public const byte Uuid = 0x0D;
}

private void ProcessXmlDoc(XElement xEntry, int spaces, StringBuilder sb) {
    string? doc = xEntry.Attribute("doc")?.Value.ToString();
    if (doc == null) return;

    for (int i = 0; i < spaces; i++) sb.Append(Spacing);
    sb.AppendLine("/// <summary>");

    foreach (string line1 in doc.Split('\n')) {
        string line = line1.Trim(' ', '*');
        if (string.IsNullOrEmpty(line)) continue;
        if(!line.EndsWith('.')) line += ".";
        line = line.Substring(0, 1).ToUpper() + line.Substring(1);
        line = HttpUtility.HtmlEncode(line);
        //line = XmlConvert.encod (line); // make sure xml string are xml compliant
        for (int i = 0; i < spaces; i++) sb.Append(Spacing);
        sb.AppendLine($"/// {line}");
    }

    for (int i = 0; i < spaces; i++) sb.Append(Spacing);
    sb.AppendLine("/// </summary>");

}

string ProcessEnum(XElement xEnum, StringBuilder sb) {
    //xEnum.Dump();
    
    ProcessXmlDoc(xEnum, 1, sb);

    string typeName = xEnum.Attribute("name")!.Value;
    sb.AppendLine($"{Spacing}public enum {typeName} {{");
    foreach(XElement xMember in xEnum.Elements()) {
        ProcessXmlDoc(xMember, 2, sb);
        string mName = xMember.Attribute("name")!.Value.ToString();
        string mValue = xMember.Attribute("value")!.Value.ToString();
        sb.AppendLine($"{Spacing}{Spacing}{mName} = {mValue},");
        sb.AppendLine();
    }
    sb.AppendLine($"{Spacing}}}");
    sb.AppendLine();
    
    return typeName;
}

private string ToCSType(string thriftType, bool isRequired, XElement? xMember, out bool isRequiredClass,
    out string? subType, out string? subTypeSubType) {

    isRequiredClass = false;
    subType = null;
    subTypeSubType = null;

    if (thriftType == "bool") return isRequired ? "bool" : "bool?";

    if (thriftType == "i8") return isRequired ? "sbyte" : "sbyte?";
    
    if (thriftType == "i16") return isRequired ? "short" : "short?";

    if (thriftType == "i32") return isRequired ? "int" : "int?";

    if (thriftType == "i64") return isRequired ? "long" : "long?";

    if (thriftType == "binary") return isRequired ? "byte[]" : "byte[]?";

    if (thriftType == "string") return isRequired ? "string" : "string?";
    //if (thriftType == "string") return "string?";
    
    if(thriftType == "id") {
        string typeId = xMember!.Attribute("type-id")!.Value.ToString();
        subType = typeId;
        if(!isRequired) typeId += "?";
        isRequiredClass = isRequired;
        return typeId;
    }
    
    if(thriftType == "list") {
        //xMember.Dump();
        
        XElement xElemType = xMember!.Element("elemType")!;
        string xElemTypeType = xElemType.Attribute("type")!.Value;
        string elemCsType;
        isRequiredClass = isRequired;

        if(xElemTypeType == "id") {
            elemCsType = xElemType.Attribute("type-id")!.Value.ToString();
            subType = "id";
            subTypeSubType = elemCsType;
        } else {
            elemCsType = ToCSType(xElemTypeType, true, xMember, out _, out _, out _);
            subType = xElemTypeType;
        }
        //xElemType.Dump();
        elemCsType = $"List<{elemCsType}>";
        if(!isRequired) elemCsType += "?";
        return elemCsType;
    }

//    if (thriftType == "struct" || thriftType == "union") {
//        return jField["type"]!["class"]!.ToString();
//    }
//
//    if (thriftType == "list") {
//
//    }

    xMember.Dump();

    return "unknown";
}

class ThriftField {
    public int id;
    public string csName;
    public string typeId;
    public string? subTypeId;
    public string? subTypeSubTypeId;
    public bool required;
}

private static readonly HashSet<string> AtomicTypeIds = new() {
    "bool", "i8", "i16", "i32", "i64", "binary", "string"
};

private bool IsAtomic(string typeId) => AtomicTypeIds.Contains(typeId);

private void GenerateAtomicFieldWriter(int fieldId, string typeId, string getter, bool required, StringBuilder sb) {
    switch (typeId) {
        case "bool":
            if (!required) getter += ".Value";
            sb.AppendLine($"proto.WriteBoolField({fieldId}, {getter});");
            break;
        case "i8":
            if (!required) getter += ".Value";
            sb.AppendLine($"proto.WriteByteField({fieldId}, {getter});");
            break;
        case "i16":
            if (!required) getter += ".Value";
            sb.AppendLine($"proto.WriteI16Field({fieldId}, {getter});");
            break;
        case "i32":
            if (!required) getter += ".Value";
            sb.AppendLine($"proto.WriteI32Field({fieldId}, {getter});");
            break;
        case "i64":
            if (!required) getter += ".Value";
            sb.AppendLine($"proto.WriteI64Field({fieldId}, {getter});");
            break;
        case "binary":
            sb.AppendLine($"proto.WriteBinaryField({fieldId}, {getter});");
            break;
        case "string":
            if (required) getter += " ?? string.Empty";
            sb.AppendLine($"proto.WriteStringField({fieldId}, {getter});");
            break;
    }
}

private string GenerateAtomicWriter(string typeId, string getter, out string elementType, out byte atomicType) {
    switch (typeId) {
        case "bool":
            elementType = "bool";
            atomicType = Types.BooleanTrue;
            return $"proto.WriteBoolValue({getter});";
        case "i64":
            elementType = "long";
            atomicType = Types.I64;
            return $"proto.WriteI64Value({getter});";
        case "binary":
            elementType = "byte[]";
            atomicType = Types.Binary;
            return $"proto.WriteBinaryValue({getter});";
        case "string":
            elementType = "string";
            atomicType = Types.Binary;
            return $"proto.WriteStringValue({getter});";
        default:
            elementType = "atom?";
            atomicType = 0;
            return "atom?";
    }
}

private string GenerateAtomicReader(string typeId, bool isField, out string csType) {
    switch (typeId) {
        case "bool":
            csType = "bool";
            return isField ? "compactType == CompactType.BooleanTrue" : "proto.ReadBool()";
        case "i8":
            csType = "sbyte";
            return $"proto.ReadByte()";
        case "i16":
            csType = "short";
            return $"proto.ReadI16()";
        case "i32":
            csType = "int";
            return $"proto.ReadI32()";
        case "i64":
            csType = "long";
            return $"proto.ReadI64()";
        case "binary":
            csType = "byte[]";
            return $"proto.ReadBinary()";
        case "string":
            csType = "string";
            return $"proto.ReadString()";
        default:
            csType = "?";
            return "atom?";
    }
}

void GenerateCompactWriter(List<ThriftField> fields, StringBuilder sb, HashSet<string> enumTypeNames) {
    sb.AppendLine();
    sb.AppendLine($"{Spacing}{Spacing}internal void Write(ThriftCompactProtocolWriter proto) {{");
    
    if(fields.Count == 0) {
        sb.AppendLine($"{Spacing}{Spacing}{Spacing}proto.WriteEmptyStruct();");
    } else {

        sb.AppendLine($"{Spacing}{Spacing}{Spacing}proto.StructBegin();");
        sb.AppendLine();
        foreach (ThriftField f in fields) {

            sb.Append($"{Spacing}{Spacing}{Spacing}");
            sb.AppendLine($"// {f.id}: {f.csName}, {f.typeId}");

            string spacing = $"{Spacing}{Spacing}{Spacing}";
            sb.Append(spacing);
            if (!f.required) {
                sb.AppendLine($"if({f.csName} != null) {{");
                spacing = $"{Spacing}{Spacing}{Spacing}{Spacing}";
                sb.Append(spacing);
            }
            string getter = f.csName;
            
            if(IsAtomic(f.typeId)) {
                GenerateAtomicFieldWriter(f.id, f.typeId, getter, f.required, sb);
            } else if (f.typeId == "id") {
                if (enumTypeNames.Contains(f.subTypeId!)) {
                    sb.AppendLine($"proto.WriteI32Field({f.id}, (int){getter});");
                } else {
                    sb.AppendLine($"proto.BeginInlineStruct({f.id});");
                    sb.AppendLine($"{spacing}{getter}.Write(proto);");
                }
            } else if (f.typeId == "list" && f.subTypeId != null) {
            
                byte atomicType;
                string elementType;
                string body;

                if (IsAtomic(f.subTypeId)) {
                    body = GenerateAtomicWriter(f.subTypeId, "element", out elementType, out atomicType);
                } else if (f.subTypeId == "id" && f.subTypeSubTypeId != null && enumTypeNames.Contains(f.subTypeSubTypeId)) {
                    elementType = f.subTypeSubTypeId;
                    atomicType = Types.I32;
                    body = $"proto.WriteI32Value((int)element);";
                } else if(f.subTypeSubTypeId != null) {
                    elementType = f.subTypeSubTypeId;
                    atomicType = Types.Struct;
                    body = $"element.Write(proto);";
                } else {
                    elementType = "?";
                    atomicType = Types.Byte;
                    body = "?";
                }

                sb.AppendLine($"proto.WriteListBegin({f.id}, {atomicType}, {getter}.Count);");
                sb.AppendLine($"{spacing}foreach({elementType} element in {getter}) {{");

                sb.Append($"{spacing}{Spacing}");
                sb.Append(body);
                sb.AppendLine();



                //sb.AppendLine($"{spacing}{Spacing}// write");
                sb.AppendLine($"{spacing}}}");
            } else {
                Console.WriteLine($"unknown type: {f.typeId} for property {f.csName} (subtype: {f.subTypeId}, {f.subTypeSubTypeId})");
                sb.AppendLine("System.Console.WriteLine(\"todo\");");
                //throw new InvalidOperationException($"unknown type: {f.typeId} for {f.csName}");

            }
            
            if(!f.required) {
                sb.AppendLine($"{Spacing}{Spacing}{Spacing}}}");
            }

            //sb.AppendLine();
        }
        sb.AppendLine();
        sb.AppendLine($"{Spacing}{Spacing}{Spacing}proto.StructEnd();");
    }
    
    sb.AppendLine($"{Spacing}{Spacing}}}");
}

void GenerateCompactReader(string enclosingTypeName, List<ThriftField> fields, StringBuilder sb, HashSet<string> enumTypeNames) {

    bool hasLists = fields.Any(f => f.typeId == "list");

    sb.AppendLine();
    sb.AppendLine($"{Spacing}{Spacing}internal static {enclosingTypeName} Read(ThriftCompactProtocolReader proto) {{");
    sb.AppendLine($"{Spacing}{Spacing}{Spacing}var r = new {enclosingTypeName}();");
    sb.AppendLine($"{Spacing}{Spacing}{Spacing}proto.StructBegin();");
    if(hasLists)
        sb.AppendLine($"{Spacing}{Spacing}{Spacing}int elementCount = 0;");
    sb.AppendLine($"{Spacing}{Spacing}{Spacing}while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {{");
    sb.AppendLine($"{Spacing}{Spacing}{Spacing}{Spacing}switch(fieldId) {{");

    foreach (ThriftField f in fields) {
        string bSpacing = $"{Spacing}{Spacing}{Spacing}{Spacing}{Spacing}{Spacing}";
        sb.AppendLine($"{Spacing}{Spacing}{Spacing}{Spacing}{Spacing}case {f.id}: // {f.csName}, {f.typeId}");

        if (IsAtomic(f.typeId)) {
            string body = GenerateAtomicReader(f.typeId, true, out _);
            sb.AppendLine($"{bSpacing}r.{f.csName} = {body};");
        } else if (f.typeId == "id") {
            if (f.subTypeId != null && enumTypeNames.Contains(f.subTypeId)) {
                // enum
                sb.AppendLine($"{bSpacing}r.{f.csName} = ({f.subTypeId})proto.ReadI32();");
            } else {
                // inline struct
                sb.AppendLine($"{bSpacing}r.{f.csName} = {f.subTypeId}.Read(proto);");
            }
        } else if (f.typeId == "list" && f.subTypeId != null) {
            // Enumerable.Range(0, proto.ReadListHeader(out _)).Select(i => [body]).ToList();
            string body;
            string elementType;
            if (IsAtomic(f.subTypeId)) {
                body = GenerateAtomicReader(f.subTypeId, false, out elementType);
            } else if (f.subTypeId == "id" && f.subTypeSubTypeId != null && enumTypeNames.Contains(f.subTypeSubTypeId)) {
                elementType = f.subTypeSubTypeId;
                body = $"({f.subTypeSubTypeId})proto.ReadI32()";
            } else {
                elementType = f.subTypeSubTypeId!;
                body = $"{f.subTypeSubTypeId}.Read(proto)";
            }

            sb.AppendLine($"{bSpacing}elementCount = proto.ReadListHeader(out _);");
            sb.AppendLine($"{bSpacing}r.{f.csName} = new List<{elementType}>(elementCount);");
            sb.AppendLine($"{bSpacing}for(int i = 0; i < elementCount; i++) {{ r.{f.csName}.Add({body}); }}");
            // the following is shorter but less performant
            //sb.AppendLine($"{bSpacing}r.{f.csName} = Enumerable.Range(0, proto.ReadListHeader(out _)).Select(i => {body}).ToList();");
        } else {
            sb.AppendLine($"{Spacing}{Spacing}{Spacing}{Spacing}{Spacing}{Spacing}r.{f.csName} = ?;");
        }
        
        sb.AppendLine($"{Spacing}{Spacing}{Spacing}{Spacing}{Spacing}{Spacing}break;");
    }

    sb.AppendLine($"{Spacing}{Spacing}{Spacing}{Spacing}{Spacing}default:");
    sb.AppendLine($"{Spacing}{Spacing}{Spacing}{Spacing}{Spacing}{Spacing}proto.SkipField(compactType);");
    sb.AppendLine($"{Spacing}{Spacing}{Spacing}{Spacing}{Spacing}{Spacing}break;");

    sb.AppendLine($"{Spacing}{Spacing}{Spacing}{Spacing}}}");
    sb.AppendLine($"{Spacing}{Spacing}{Spacing}}}");
    sb.AppendLine($"{Spacing}{Spacing}{Spacing}proto.StructEnd();");
    sb.AppendLine($"{Spacing}{Spacing}{Spacing}return r;");
    sb.AppendLine($"{Spacing}{Spacing}}}");
}

string ProcessStruct(XElement xStruct, StringBuilder sb, HashSet<string> enumTypeNames) {
    var fields = new List<ThriftField>();
    //xStruct.Dump();
    ProcessXmlDoc(xStruct, 1, sb);
    string typeName = xStruct.Attribute("name")!.Value;
    sb.AppendLine($"{Spacing}public class {typeName} {{");
    foreach (XElement xMember in xStruct.Elements()) {
        ProcessXmlDoc(xMember, 2, sb);
        string mName = xMember.Attribute("name")!.Value.ToString();
        string mFieldId = xMember.Attribute("field-id")!.Value.ToString();
        bool required = xMember.Attribute("required")!.Value.ToString() == "required";
        string mTypeId = xMember.Attribute("type")!.Value.ToString();
        string csType = ToCSType(mTypeId, required, xMember, out bool isRequiredClass, out string? subType, out string? subTypeSubType);
        string csName = mName.Pascalize();
        sb.Append($"{Spacing}{Spacing}public {csType} {csName} {{ get; set; }}");
        if (isRequiredClass) {
            sb.Append($" = new {csType}();");
        }
        else if (csType == "string" && required) {
            sb.Append(" = string.Empty;");
        }
        sb.AppendLine();
        sb.AppendLine();
        
        // gather metadata
        fields.Add(new ThriftField {
            id = int.Parse(mFieldId),
            csName = csName,
            typeId = mTypeId,
            required = required,
            subTypeId = subType,
            subTypeSubTypeId = subTypeSubType
        });
    }
    
    GenerateCompactWriter(fields, sb, enumTypeNames);
    GenerateCompactReader(typeName, fields, sb, enumTypeNames);
    
    sb.AppendLine($"{Spacing}}}");
    sb.AppendLine();
    
    return typeName;
}

void Main() {
    XElement xRoot = XElement.Parse(File.ReadAllText(inputPath));
    
    XElement? xDoc = xRoot.XPathSelectElement("document");
    if(xDoc == null) throw new InvalidOperationException("no document element found");
    
    
    var sb = new StringBuilder();
    sb.AppendLine("#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member");
    sb.AppendLine("using System.Linq;");
    sb.AppendLine("using System.Collections.Generic;");
    sb.AppendLine("using Parquet.Meta.Proto;");
    sb.AppendLine("namespace Parquet.Meta {");
    
    var enumTypeNames = new HashSet<string>();
    var classTypeNames = new HashSet<string>();
    
    foreach(XElement xItem in xDoc.Elements()) {
        string xItemName = xItem.Name.ToString();
        
        switch (xItemName) {
            case "namespace":
                break;
            case "enum":
                enumTypeNames.Add(ProcessEnum(xItem, sb));
                break;
            case "struct":
                classTypeNames.Add(ProcessStruct(xItem, sb, enumTypeNames));
                break;
            case "union":
                classTypeNames.Add(ProcessStruct(xItem, sb, enumTypeNames));
                break;
            default:
                xItem.Dump();
                throw new InvalidOperationException($"don't know how to process {xItemName}");
        }
    }
    
    sb.AppendLine("}");
    sb.AppendLine("#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member");
    
    // save to file
    File.WriteAllText(outputDir + "Parquet.cs", sb.ToString());
}
