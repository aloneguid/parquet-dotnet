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
        line = HttpUtility.HtmlEncode(line);
        //line = XmlConvert.encod (line); // make sure xml string are xml compliant
        for (int i = 0; i < spaces; i++) sb.Append(Spacing);
        sb.AppendLine($"/// {line}");
    }

    for (int i = 0; i < spaces; i++) sb.Append(Spacing);
    sb.AppendLine("/// </summary>");

}

void ProcessEnum(XElement xEnum, StringBuilder sb) {
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
}

private string ToCSType(string thriftType, bool isRequired, XElement xMember, out bool isRequiredClass) {

    isRequiredClass = false;

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
        } else {
            elemCsType = ToCSType(xElemTypeType, true, xMember, out _);
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

void ProcessStruct(XElement xStruct, StringBuilder sb) {
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
        string csType = ToCSType(mTypeId, required, xMember, out bool isRequiredClass);
        string csName = mName.Pascalize();
        sb.Append($"{Spacing}{Spacing}public {csType} {csName} {{ get; set; }}");
        if (isRequiredClass) sb.Append($" = new {csType}();");
        sb.AppendLine();
        sb.AppendLine();
    }
    sb.AppendLine($"{Spacing}}}");
    sb.AppendLine();
}

void Main() {
    XElement xRoot = XElement.Parse(File.ReadAllText(inputPath));
    
    XElement? xDoc = xRoot.XPathSelectElement("document");
    if(xDoc == null) throw new InvalidOperationException("no document element found");
    
    
    var sb = new StringBuilder();
    sb.AppendLine("#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member");
    sb.AppendLine("using System.Collections.Generic;");
    sb.AppendLine("namespace Parquet.Meta {");
    
    foreach(XElement xItem in xDoc.Elements()) {
        string xItemName = xItem.Name.ToString();
        
        switch (xItemName) {
            case "namespace":
                break;
            case "enum":
                ProcessEnum(xItem, sb);
                break;
            case "struct":
                ProcessStruct(xItem, sb);
                break;
            case "union":
                ProcessStruct(xItem, sb);
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
