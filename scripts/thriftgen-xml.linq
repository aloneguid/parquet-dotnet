<Query Kind="Program">
  <NuGetReference>Humanizer</NuGetReference>
  <Namespace>Humanizer</Namespace>
  <Namespace>System.Web</Namespace>
</Query>

#nullable enable

const string Spacing = "    ";
const string inputPath = @"C:\dev\parquet-dotnet\src\Parquet\Meta\parquet.xml";
const string outputDir = @"C:\dev\parquet-dotnet\src\Parquet\Meta\";

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

private string ToCSType(string thriftType, bool isRequired, XElement xMember, out bool isRequiredClass,
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

    //if (thriftType == "string") return isRequired ? "string" : "string?";
    if (thriftType == "string") return "string?";
    
    if(thriftType == "id") {
        string typeId = xMember.Attribute("type-id")!.Value.ToString();
        subType = typeId;
        if(!isRequired) typeId += "?";
        isRequiredClass = isRequired;
        return typeId;
    }
    
    if(thriftType == "list") {
        //xMember.Dump();
        
        XElement xElemType = xMember.Element("elemType")!;
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

void GenerateCompactWriter(List<ThriftField> fields, StringBuilder sb, HashSet<string> enumTypeNames) {
    sb.AppendLine($"{Spacing}{Spacing}internal void Write(ThriftCompactProtocolWriter proto) {{");
    
    if(fields.Count == 0) {
        sb.AppendLine($"{Spacing}{Spacing}{Spacing}proto.StructEmpty();");
    } else {

        sb.AppendLine($"{Spacing}{Spacing}{Spacing}proto.StructBegin();");
        sb.AppendLine();
        foreach (ThriftField f in fields) {

            sb.Append($"{Spacing}{Spacing}{Spacing}");
            sb.AppendLine($"// {f.id}: {f.csName}, {f.typeId}");

            sb.Append($"{Spacing}{Spacing}{Spacing}");
            if (!f.required) {
                sb.AppendLine($"if({f.csName} != null)");
                sb.Append($"{Spacing}{Spacing}{Spacing}{Spacing}");
            }
            string getter = f.csName;

            switch (f.typeId) {
                case "bool":
                    if (!f.required) getter += ".Value";
                    sb.AppendLine($"proto.WriteBoolField({f.id}, {getter});");
                    break;
                case "i8":
                    if (!f.required) getter += ".Value";
                    sb.AppendLine($"proto.WriteByteField({f.id}, {getter});");
                    break;
                case "i16":
                    if (!f.required) getter += ".Value";
                    sb.AppendLine($"proto.WriteI16Field({f.id}, {getter});");
                    break;
                case "i32":
                    if (!f.required) getter += ".Value";
                    sb.AppendLine($"proto.WriteI32Field({f.id}, {getter});");
                    break;
                case "i64":
                    if (!f.required) getter += ".Value";
                    sb.AppendLine($"proto.WriteI64Field({f.id}, {getter});");
                    break;
                case "binary":
                    sb.AppendLine($"proto.WriteBinaryField({f.id}, {getter});");
                    break;
                case "string":
                    if(f.required) getter += " ?? string.Empty";
                    sb.AppendLine($"proto.WriteStringField({f.id}, {getter});");
                    break;
                    
                case "id":
                    if (enumTypeNames.Contains(f.subTypeId!)) {
                        sb.AppendLine($"proto.WriteI32Field({f.id}, (int){getter});");
                    } else {
                        sb.Append("{ ");
                        sb.Append($"proto.BeginInlineStruct({f.id});");
                        sb.Append(" ");
                        sb.Append($"{getter}.Write(proto);");
                        sb.AppendLine(" }");
                    }
                    break;

                //case "list":
                //if(f.subTypeId == "id"
                //break;

                default:
                    Console.WriteLine($"unknown type: {f.typeId} for property {f.csName} (subtype: {f.subTypeId}, {f.subTypeSubTypeId})");
                    sb.AppendLine("System.Console.WriteLine(\"todo\");");
                    //throw new InvalidOperationException($"unknown type: {f.typeId} for {f.csName}");
                    break;
            }

            //sb.AppendLine();
        }
        sb.AppendLine();
        sb.AppendLine($"{Spacing}{Spacing}{Spacing}proto.StructEnd();");
    }
    
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
        if (isRequiredClass) sb.Append($" = new {csType}();");
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
