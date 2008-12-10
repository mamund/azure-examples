using System;
using System.Collections.Generic;
using System.Text;
using Amundsen.Utilities;
using System.Text.RegularExpressions;
using System.Web;
using System.Net;
using System.IO;

namespace Amundsen.Azure.CommandLine
{
  /// <summary>
  /// Public Domain 2008 amundsen.com, inc.
  /// @author   mike amundsen (mamund@yahoo.com)
  /// @version  1.0 (2008-12-08)
  /// @notes    2008-12-08 :  this is really early stuff. likely it doesn't do what you want. 
  ///                         use it if you like, but don't complain. make it better and tell
  ///                         me all about it. (mca)
  /// @version  1.0 (2008-12-09)
  /// @notes    2008-12-09 :  cleaned up POST/PUT coding. added support for properties.xml
  ///                         updated the ShowHelp() text.
  /// </summary>
  class AzureConsole
  {
    static string azureAccount = string.Empty;
    static string azureEndPoint = string.Empty;
    static string azureSharedKey = string.Empty;

    static string table_regex = "^/([^/]*)$";
    static string entity_regex = "^/([^/]*)/(([^,]*),(.*))?$";
    static string query_regex = "^/([^/]*)/\\?(.*)$"; 

    static void Main(string[] args)
    {
      AzureCommands ac = new AzureCommands();
      string uri = string.Empty;
      string cmd = string.Empty;
      string[] arglist = args;

      try
      {
        if (arglist.Length == 0)
        {
          ShowHelp();
          return;
        }

        HandleConfigSettings();
        ac.Account = azureAccount;
        ac.EndPoint = azureEndPoint;
        ac.SharedKey = azureSharedKey;

        uri = arglist[0];
        cmd = (args.Length == 1 ? "get" : (uri.IndexOf("?") == -1 ? arglist[1] : "get"));

        // authority command
        if (Regex.IsMatch(uri, table_regex, RegexOptions.IgnoreCase))
        {
          ac.Tables(new string[] { cmd, uri.Replace("/", "") });
          return;
        }

        // entity command
        if (Regex.IsMatch(uri, entity_regex, RegexOptions.IgnoreCase))
        {
          uri = uri.Replace(",", "/");
          string[] elm = uri.Split('/');
          ac.Entities(new string[] { cmd, elm[1], elm[2], (elm.Length > 3 ? elm[3] : string.Empty), (arglist.Length > 2 ? arglist[2] : string.Empty) });
          return;
        }

        // query command
        if (Regex.IsMatch(uri, query_regex, RegexOptions.IgnoreCase))
        {
          string[] elm = uri.Split('/');
          ac.Queries(new string[] { cmd, elm[1], elm[2], arglist[1] });
          return;
        }

        // failed to recognize command uri
        Console.Out.WriteLine("***ERROR: unable to parse command line!");
        ShowHelp();
        return;

      }
      catch (HttpException hex)
      {
        Console.Out.WriteLine(string.Format("\n***ERROR: {0} : {1}\n", hex.GetHttpCode(),hex.Message));
      }
      catch (Exception ex)
      {
        Console.Out.WriteLine(string.Format("\n***ERROR: {0}\n", ex.Message));
      }
    }

    private static void ShowHelp()
    {
      Console.Out.WriteLine("\nAzure Table Storage Console (1.0 - 2008-12-07)\n");

      Console.Out.WriteLine("Tables:");
      Console.Out.WriteLine("\t/{tid} [[g]et]\n\tex: /my-table\n");
      Console.Out.WriteLine("\t/{tid} [p]ost\n\tex: /my-new-table p\n");

      Console.Out.WriteLine("Entities:");
      Console.Out.WriteLine("\t/{tid}/ [[g]et]\n\tex: /my-table/\n");
      Console.Out.WriteLine("\t/{tid}/{pid},{rid} [[g]et]\n\tex: /my-table/my-partition,my-row\n");
      Console.Out.WriteLine("\t/{tid}/{pid},{rid} \"{xml}|{filename}\" [p]ost\n\tex: /my-table/my-partition,myrow c:\\new-properties.xml p\n");
      Console.Out.WriteLine("\t/{tid}/{pid},{rid} \"{xml|filename}\" p[u]t\n\tex: /my-table/my-partition,my-row c:\\modified-properties.xml u\n");
      Console.Out.WriteLine("\t/{tid}/{pid},{rid} [d]elete\n\tex: /my-table/my-partition,my-row d\n");

      Console.Out.WriteLine("Queries:");
      Console.Out.WriteLine("\t/{tid}/? \"{query}\" [[g]et]\n\tex: /my-table/? \"[valid Astoria query]\"\n");
    }

    private static void HandleConfigSettings()
    {
      WebUtility wu = new WebUtility();
      azureAccount = wu.GetConfigSectionItem("azureSettings", "azureAccount");
      azureEndPoint = wu.GetConfigSectionItem("azureSettings", "azureEndPoint");
      azureSharedKey = wu.GetConfigSectionItem("azureSettings", "azureSharedKey");
    }
  }

  class AzureCommands
  {
    HttpClient client = new HttpClient();
    Hashing h = new Hashing();

    public AzureCommands()
    {
      client.UserAgent = "amundsen/1.0";
    }
    public AzureCommands(string account, string endPoint, string sharedKey)
    {
      this.Account = account;
      this.EndPoint = endPoint;
      this.SharedKey = sharedKey;
      client.UserAgent = "amundsen/1.0";
    }

    public string Account = string.Empty;
    public string EndPoint = string.Empty;
    public string SharedKey = string.Empty;
    public string ETag = string.Empty;

    private string contentType = "application/atom+xml";
    private string keyType = "SharedKey";
    private DateTime requestDate = DateTime.UtcNow;
    private string contentMD5 = string.Empty;
    private string fmtHeader = "{0} {1}:{2}";
    private string fmtStringToSign = "{0}\n{1}\n{2}\n{3:R}\n{4}";

    private string authValue = string.Empty;
    private string sigValue = string.Empty;
    private string authHeader = string.Empty;
    private string method = string.Empty;

    public void Tables(string[] args)
    {
      string body = string.Empty;
      string url = string.Empty;
      int cmd = 0;
      int table = 1;

      string canonicalResource = string.Format("/{0}/{1}", this.Account, "Tables");
      string requestUrl = string.Format("{0}/{1}", this.EndPoint, "Tables");
      requestDate = DateTime.UtcNow;

      switch (args[cmd].ToLower())
      {
        case "g":
        case "get":
          method = "GET";
          
          if (args[table] != string.Empty)
          {
            canonicalResource += string.Format("('{0}')", args[table]);
            requestUrl += string.Format("('{0}')", args[table]);
          }
          
          authValue = string.Format(fmtStringToSign, method, contentMD5, contentType, requestDate, canonicalResource);
          sigValue = h.MacSha(authValue, Convert.FromBase64String(this.SharedKey));
          authHeader = string.Format(fmtHeader, keyType, this.Account, sigValue);

          client.RequestHeaders.Add("x-ms-date", string.Format("{0:R}", requestDate));
          client.RequestHeaders.Add("authorization", authHeader);

          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType));
          this.ETag = client.ResponseHeaders["etag"];

          break;

        case "p":
        case "post":
          method = "POST";
          string rtnBody = string.Empty;

          // use stub body and insert table name
          body = string.Format(createTableXml, requestDate, args[table]);
          contentMD5 = h.MD5(body);
          
          authValue = string.Format(fmtStringToSign, method, contentMD5, contentType, requestDate, canonicalResource);
          sigValue = h.MacSha(authValue, Convert.FromBase64String(this.SharedKey));
          authHeader = string.Format(fmtHeader, keyType, this.Account, sigValue);

          client.RequestHeaders.Add("Content-MD5", contentMD5);
          client.RequestHeaders.Add("x-ms-date", string.Format("{0:R}", requestDate));
          client.RequestHeaders.Add("authorization", authHeader);
          
          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType, body));
          Console.Out.WriteLine(string.Format("Table [{0}] has been added.", (client.ResponseHeaders["location"] != null ? client.ResponseHeaders["location"] : args[table])));

          break;

        case "d":
        case "delete":
          method = "DELETE";
          
          if (args[table] != string.Empty)
          {
            canonicalResource += string.Format("('{0}')", args[table]);
            requestUrl += string.Format("('{0}')", args[table]);
          }
          
          authValue = string.Format(fmtStringToSign, method, contentMD5, contentType, requestDate, canonicalResource);
          sigValue = h.MacSha(authValue, Convert.FromBase64String(this.SharedKey));
          authHeader = string.Format(fmtHeader, keyType, this.Account, sigValue);

          client.RequestHeaders.Add("x-ms-date", string.Format("{0:R}", requestDate));
          client.RequestHeaders.Add("authorization", authHeader);

          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType));
          Console.Out.WriteLine(string.Format("Table [{0}] has been deleted.", (client.ResponseHeaders["location"] != null ? client.ResponseHeaders["location"] : args[table])));
          
          break;

        default:
          throw new ApplicationException("Invalid Table Command [" + args[cmd] + "]");
      }
    }

    public void Entities(string[] args)
    {
      string body = string.Empty;
      string readBody = string.Empty;
      string rtnBody = string.Empty;
      string url = string.Empty;
      int cmd = 0;
      int table = 1;
      int partition = 2;
      int row = 3;
      int doc = 4;

      string canonicalResource = string.Format("/{0}/{1}", this.Account, args[table]);
      string requestUrl = string.Format("{0}/{1}", this.EndPoint, args[table]);
      requestDate = DateTime.UtcNow;

      switch (args[cmd].ToLower())
      {
        case "g":
        case "get":
          method = "GET";
          
          if (args.Length > 2 && args[partition]!=string.Empty && args[row]!=string.Empty)
          {
            canonicalResource += string.Format("(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
            requestUrl += string.Format("(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
          }
          else
          {
            canonicalResource += string.Format("()");
            requestUrl += string.Format("()");
          }
          
          authValue = string.Format(fmtStringToSign, method, contentMD5, contentType, requestDate, canonicalResource);
          sigValue = h.MacSha(authValue, Convert.FromBase64String(this.SharedKey));
          authHeader = string.Format(fmtHeader, keyType, this.Account, sigValue);

          client.RequestHeaders.Add("x-ms-date", string.Format("{0:R}", requestDate));
          client.RequestHeaders.Add("authorization", authHeader);

          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType));
          this.ETag = client.ResponseHeaders["etag"];

          break;

        case "p":
        case "post":
          method = "POST";
          rtnBody = string.Empty;
          
          // get data from command line and build up body
          readBody = ResolveDocument(args[doc]);
          body = string.Format(createEntityXml, requestDate, readBody);
          body = string.Format(body, args[partition], args[row]);
          contentMD5 = h.MD5(body);

          authValue = string.Format(fmtStringToSign, method, contentMD5, contentType, requestDate, canonicalResource);
          sigValue = h.MacSha(authValue, Convert.FromBase64String(this.SharedKey));
          authHeader = string.Format(fmtHeader, keyType, this.Account, sigValue);

          client.RequestHeaders.Add("Content-MD5", contentMD5);
          client.RequestHeaders.Add("x-ms-date", string.Format("{0:R}", requestDate));
          client.RequestHeaders.Add("authorization", authHeader);
          
          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType, body));
          Console.Out.WriteLine(string.Format("Entity [{0}] has been added.", (client.ResponseHeaders["location"] != null ? client.ResponseHeaders["location"] : string.Format("{0}/{1},{2}", args[table], args[partition], args[row]))));

          break;

        case "u":
        case "put":
          method = "PUT";
          
          if (args.Length > 2)
          {
            canonicalResource += string.Format("(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
            requestUrl += string.Format("(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
          }

          // build hash to GET for Etag;
          authValue = string.Format(fmtStringToSign, "GET", contentMD5, contentType, requestDate, canonicalResource);
          sigValue = h.MacSha(authValue, Convert.FromBase64String(this.SharedKey));
          authHeader = string.Format(fmtHeader, keyType, this.Account, sigValue);
          
          client.RequestHeaders.Add("x-ms-date", string.Format("{0:R}", requestDate));
          client.RequestHeaders.Add("authorization", authHeader);

          // get rec to record etag
          rtnBody = client.Execute(requestUrl, "GET", contentType);
          this.ETag = client.ResponseHeaders["etag"];

          // accept input doc and parse into valid Atom for Azure Tables
          rtnBody = string.Empty;
          readBody = ResolveDocument(args[doc]);
          body = string.Format(updateEntityXml, args[table], args[partition], args[row], requestDate, readBody,this.ETag.Replace(@"""","&quot;"),this.Account);
          contentMD5 = h.MD5(body);

          // build tha hash value
          authValue = string.Format(fmtStringToSign, method, contentMD5, contentType, requestDate, canonicalResource);
          sigValue = h.MacSha(authValue, Convert.FromBase64String(this.SharedKey));
          authHeader = string.Format(fmtHeader, keyType, this.Account, sigValue);
          
          client.RequestHeaders["x-ms-date"]=string.Format("{0:R}", requestDate);
          client.RequestHeaders["authorization"] =authHeader;
          client.RequestHeaders.Add("content-md5", contentMD5);
          client.RequestHeaders.Add("if-match", this.ETag);

          // PUT to the server & report results
          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType, body));
          Console.Out.WriteLine(string.Format("Entity [{0}] has been updated.", (client.ResponseHeaders["location"] != null ? client.ResponseHeaders["location"] : string.Format("{0}/{1},{2}", args[table], args[partition], args[row]))));
          
          break;

        case "d":
        case "delete":
          method = "DELETE";
          
          if (args.Length>2)
          {
            canonicalResource += string.Format("(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
            requestUrl += string.Format("(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
          }

          // build up GET to recover Etag;
          authValue = string.Format(fmtStringToSign, "GET", contentMD5, contentType, requestDate, canonicalResource);
          sigValue = h.MacSha(authValue, Convert.FromBase64String(this.SharedKey));
          authHeader = string.Format(fmtHeader, keyType, this.Account, sigValue);

          client.RequestHeaders.Add("x-ms-date", string.Format("{0:R}", requestDate));
          client.RequestHeaders.Add("authorization", authHeader);

          // get rec to record etag
          rtnBody = client.Execute(requestUrl, "GET", contentType);
          this.ETag = client.ResponseHeaders["etag"];
          contentMD5 = string.Empty;

          // now build up DELETE to finish the job
          authValue = string.Format(fmtStringToSign, method, contentMD5, contentType, requestDate, canonicalResource);
          sigValue = h.MacSha(authValue, Convert.FromBase64String(this.SharedKey));
          authHeader = string.Format(fmtHeader, keyType, this.Account, sigValue);

          client.RequestHeaders["x-ms-date"] = string.Format("{0:R}", requestDate);
          client.RequestHeaders["authorization"] = authHeader;
          client.RequestHeaders.Add("if-match", this.ETag);

          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType));
          Console.Out.WriteLine(string.Format("Entity [{0}] has been deleted.", (client.ResponseHeaders["location"] != null ? client.ResponseHeaders["location"] : string.Format("{0}/{1},{2}",args[table],args[partition],args[row]))));

          break;

        default:
          throw new ApplicationException("Invalid Entity Command [" + args[cmd] + "]");
      }
    }

    // cool query parser goes here.
    public void Queries(string[] args)
    {
    }

    // collect properties xml (from disk, if needed)
    private string ResolveDocument(string doc)
    {
      string rtn = string.Empty;

      // passed as literal string?
      if (doc.IndexOf("<m:properties") != -1)
      {
        rtn = doc;
      }
      else
      {
        // must be a file pointer
        if (doc.IndexOf(":") == -1)
        {
          doc = Directory.GetCurrentDirectory() + @"\"+ doc;
        }
        using (System.IO.StreamReader sr = new System.IO.StreamReader(doc,Encoding.UTF8))
        {
          rtn = sr.ReadToEnd();
          sr.Close();
        }
      }
      return rtn;
    }

    // stub create table body
    string createTableXml = @"<?xml version=""1.0"" encoding=""utf-8"" standalone=""yes""?>
      <entry 
        xmlns:d=""http://schemas.microsoft.com/ado/2007/08/dataservices"" 
        xmlns:m=""http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"" 
        xmlns=""http://www.w3.org/2005/Atom"">
        <title />
        <updated>{0:yyyy-MM-ddTHH:mm:ss.fffffffZ}</updated>
        <author>
          <name />
        </author>
        <id />
        <content type=""application/xml"">
          <m:properties>
            <d:TableName>{1}</d:TableName>
          </m:properties>
        </content>
      </entry>";


    // stub create entity body
    string createEntityXml = @"<?xml version=""1.0"" encoding=""utf-8"" standalone=""yes""?>
      <entry 
        xmlns:d=""http://schemas.microsoft.com/ado/2007/08/dataservices"" 
        xmlns:m=""http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"" 
        xmlns=""http://www.w3.org/2005/Atom"">
        <title />
        <updated>{0:yyyy-MM-ddTHH:mm:ss.fffffffZ}</updated>
        <author>
          <name />
        </author>
        <id />
        <content type=""application/xml"">
          {1}
        </content>
      </entry>";

    /* sample body to pass in via string or file ref
    <m:properties>
      <d:PartitionKey>{0}</d:PartitionKey>
      <d:RowKey>{1}</d:RowKey>
      <d:Address>Mountain View</d:Address>
      <d:Age m:type=""Edm.Int32"">23</d:Age>
      <d:AmountDue m:type=""Edm.Double"">200.23</d:AmountDue>
      <d:BinaryData m:type=""Edm.Binary"" m:null=""true"" />
      <d:CustomerCode m:type=""Edm.Guid"">c9da6455-213d-42c9-9a79-3e9149a57833</d:CustomerCode>
      <d:CustomerSince m:type=""Edm.DateTime"">2008-07-10T00:00:00</d:CustomerSince>
      <d:IsActive m:type=""Edm.Boolean"">true</d:IsActive>
      <d:NumOfOrders m:type=""Edm.Int64"">255</d:NumOfOrders>
      <d:Timestamp m:type=""Edm.DateTime"">0001-01-01T00:00:00</d:Timestamp>
    </m:properties>
    */

    // stub update entity body
    string updateEntityXml = @"<?xml version=""1.0"" encoding=""utf-8"" standalone=""yes""?>
      <entry 
        xml:base=""http://mamund.table.core.windows.net/"" 
        xmlns:d=""http://schemas.microsoft.com/ado/2007/08/dataservices"" 
        xmlns:m=""http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"" 
        m:etag=""{5}"" 
        xmlns=""http://www.w3.org/2005/Atom"">
        <id>http://{6}.table.core.windows.net/{0}(PartitionKey='{1}',RowKey='{2}')</id>
        <title type=""text""></title>
        <updated>{3:yyyy-MM-ddTHH:mm:ss.fffffffZ}</updated>
        <author>
          <name />
        </author>
        <link rel=""edit""  href=""{0}(PartitionKey='{1}',RowKey='{2}')"" />
        <category term=""{6}.{0}"" scheme=""http://schemas.microsoft.com/ado/2007/08/dataservices/scheme"" />
        <content type=""application/xml"">
          {4}
        </content>
      </entry>";
  }
}
