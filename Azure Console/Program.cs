using System;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using System.Net;
using System.IO;

using Amundsen.Utilities;

namespace Amundsen.Azure.CommandLine
{
  /// <summary>
  /// Public Domain 2008 amundsen.com, inc.
  /// @author   mike amundsen (mamund@yahoo.com)
  /// 
  /// @version  1.0c (2008-12-10)
  /// @notes    added support for MERGE and ad-hod queries
  ///           refactored the key-signing and other code.
  ///           updated ShowHelp() text
  /// 
  /// @version  1.0b (2008-12-09)
  /// @notes    cleaned up POST/PUT coding. added support for properties.xml
  ///           updated the ShowHelp() text.
  /// 
  /// @version  1.0 (2008-12-08)
  /// @notes    this is really early stuff. likely it doesn't do what you want. 
  ///           use it if you like, but don't complain. make it better and tell
  ///           me all about it. (mca)
  /// </summary>
  class AzureConsole
  {
    static string azureAccount = string.Empty;
    static string azureEndPoint = string.Empty;
    static string azureSharedKey = string.Empty;

    static string table_regex = "^/([^/]*)$";
    static string entity_regex = "^/([^/]*)/(([^,]*),(.*))?$";
    static string query_regex = @"^\?(.*)$"; 

    // handler user interaction
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
        cmd = (args.Length == 1 ? "get" : (uri.IndexOf("?", StringComparison.CurrentCultureIgnoreCase) == -1 ? arglist[1] : "get"));

        // table command
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
          uri = uri.Substring(1);
          string[] elm = { uri };
          ac.Queries(new string[] { cmd, elm[0]});
          return;
        }

        // failed to recognize command uri
        Console.Out.WriteLine("***ERROR: unable to parse command line!");
        ShowHelp();

        return;
        }
      catch (HttpException hex)
      {
        Console.Out.WriteLine(string.Format(CultureInfo.CurrentCulture,"\n***ERROR: {0} : {1}\n", hex.GetHttpCode(),hex.Message));
      }
      catch (Exception ex)
      {
        Console.Out.WriteLine(string.Format(CultureInfo.CurrentCulture,"\n***ERROR: {0}\n", ex.Message));
      }
    }

    private static void ShowHelp()
    {
      Console.Out.WriteLine("\nAzure Table Storage Console (1.0b - 2008-12-09)\n");

      Console.Out.WriteLine("Tables:");
      Console.Out.WriteLine("\t/{tid} [[g]et]\n\tex: /my-table\n");
      Console.Out.WriteLine("\t/{tid} [p]ost\n\tex: /my-new-table p\n");

      Console.Out.WriteLine("Entities:");
      Console.Out.WriteLine("\t/{tid}/ [[g]et]\n\tex: /my-table/\n");
      Console.Out.WriteLine("\t/{tid}/{pid},{rid} [[g]et]\n\tex: /my-table/my-partition,my-row\n");
      Console.Out.WriteLine("\t/{tid}/{pid},{rid} \"{xml}|{filename}\" [p]ost\n\tex: /my-table/my-partition,myrow c:\\new-properties.xml p\n");
      Console.Out.WriteLine("\t/{tid}/{pid},{rid} \"{xml|filename}\" p[u]t\n\tex: /my-table/my-partition,my-row c:\\modified-properties.xml u\n");
      Console.Out.WriteLine("\t/{tid}/{pid},{rid} \"{xml|filename}\" [m]erge\n\tex: /my-table/my-partition,my-row c:\\partial-properties.xml u\n");
      Console.Out.WriteLine("\t/{tid}/{pid},{rid} [d]elete\n\tex: /my-table/my-partition,my-row d\n");

      Console.Out.WriteLine("Queries:");
      Console.Out.WriteLine("\t\"?{query}\" [[g]et]\n\tex: \"?Customers()?$filter=(Region eq 'north')\"\n");
    }

    private static void HandleConfigSettings()
    {
      WebUtility wu = new WebUtility();
      azureAccount = wu.GetConfigSectionItem("azureSettings", "azureAccount");
      azureEndPoint = wu.GetConfigSectionItem("azureSettings", "azureEndPoint");
      azureSharedKey = wu.GetConfigSectionItem("azureSettings", "azureSharedKey");
    }
  }

  // the real work is done here
  class AzureCommands
  {
    HttpClient client = new HttpClient();
    Hashing h = new Hashing();

    public AzureCommands()
    {
      client.UserAgent = "amundsen/1.0";
    }
    public AzureCommands(string account, string endPoint, string sharedKey, string keyType)
    {
      this.Account = account;
      this.EndPoint = endPoint;
      this.SharedKey = sharedKey;
      this.KeyType = keyType;
      client.UserAgent = "amundsen/1.0";
    }

    public string Account = string.Empty;
    public string EndPoint = string.Empty;
    public string SharedKey = string.Empty;
    public string KeyType = "SharedKey";
    public string ETag = string.Empty;

    private string contentType = "application/atom+xml";
    private DateTime requestDate = DateTime.UtcNow;
    private string contentMD5 = string.Empty;
    private string authHeader = string.Empty;
    private string method = string.Empty;

    public void Tables(string[] args)
    {
      string sendBody = string.Empty;
      string rtnBody = string.Empty;
      int cmd = 0;
      int table = 1;

      string canonicalResource = string.Format(CultureInfo.CurrentCulture,"/{0}/{1}", this.Account, "Tables");
      string requestUrl = string.Format(CultureInfo.CurrentCulture,"{0}/{1}", this.EndPoint, "Tables");
      requestDate = DateTime.UtcNow;

      switch (args[cmd].ToLower())
      {
        case "g":
        case "get":
          method = "GET";
          
          // single tale or all tables?
          if (args[table] != string.Empty)
          {
            // single table
            canonicalResource += string.Format(CultureInfo.CurrentCulture,"('{0}')", args[table]);
            requestUrl += string.Format(CultureInfo.CurrentCulture,"('{0}')", args[table]);
          }
          
          // do GET
          authHeader = CreateSharedKeyAuth(method, canonicalResource, contentMD5, requestDate);
          client.RequestHeaders["x-ms-date"] = string.Format(CultureInfo.CurrentCulture,"{0:R}", requestDate);
          client.RequestHeaders["authorization"] = authHeader;

          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType));
          this.ETag = client.ResponseHeaders["etag"];

          break;

        case "p":
        case "post":
          method = "POST";

          // build valid Atom document
          sendBody = string.Format(createTableXml, requestDate, args[table]);
          contentMD5 = h.MD5(sendBody);

          // do POST
          authHeader = CreateSharedKeyAuth(method, canonicalResource, contentMD5, requestDate);
          client.RequestHeaders["Content-MD5"] = contentMD5;
          client.RequestHeaders["x-ms-date"] = string.Format(CultureInfo.CurrentCulture,"{0:R}", requestDate);
          client.RequestHeaders["authorization"] = authHeader;
          
          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType, sendBody));
          Console.Out.WriteLine(string.Format(CultureInfo.CurrentCulture,"Table [{0}] has been added.", (client.ResponseHeaders["location"] != null ? client.ResponseHeaders["location"] : args[table])));

          break;

        case "d":
        case "delete":
          method = "DELETE";
          
          // build up uri
          if (args[table] != string.Empty)
          {
            canonicalResource += string.Format(CultureInfo.CurrentCulture, "('{0}')", args[table]);
            requestUrl += string.Format(CultureInfo.CurrentCulture, "('{0}')", args[table]);
          }
          else
          {
            throw new HttpException(400, "Missing TableName");
          }
          
          // do DELETE
          authHeader = CreateSharedKeyAuth(method, canonicalResource, contentMD5, requestDate);
          client.RequestHeaders["x-ms-date"] = string.Format(CultureInfo.CurrentCulture,"{0:R}", requestDate);
          client.RequestHeaders["authorization"] = authHeader;

          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType));
          Console.Out.WriteLine(string.Format(CultureInfo.CurrentCulture,"Table [{0}] has been deleted.", (client.ResponseHeaders["location"] != null ? client.ResponseHeaders["location"] : args[table])));
          
          break;

        default:
          throw new ApplicationException("Invalid Table Command [" + args[cmd] + "]");
      }
    }

    public void Entities(string[] args)
    {
      string sendBody = string.Empty;
      string readBody = string.Empty;
      int cmd = 0;
      int table = 1;
      int partition = 2;
      int row = 3;
      int doc = 4;

      string canonicalResource = string.Format(CultureInfo.CurrentCulture,"/{0}/{1}", this.Account, args[table]);
      string requestUrl = string.Format(CultureInfo.CurrentCulture,"{0}/{1}", this.EndPoint, args[table]);
      requestDate = DateTime.UtcNow;

      switch (args[cmd].ToLower())
      {
        case "g":
        case "get":
          method = "GET";
          
          // work out URI format
          if (args.Length > 2 && args[partition]!=string.Empty && args[row]!=string.Empty)
          {
            // single entity
            canonicalResource += string.Format(CultureInfo.CurrentCulture,"(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
            requestUrl += string.Format(CultureInfo.CurrentCulture,"(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
          }
          else
          {
            // all entities
            canonicalResource += string.Format(CultureInfo.CurrentCulture,"()");
            requestUrl += string.Format(CultureInfo.CurrentCulture,"()");
          }
          
          // do GET
          authHeader = CreateSharedKeyAuth(method, canonicalResource, contentMD5, requestDate);
          client.RequestHeaders["x-ms-date"] = string.Format(CultureInfo.CurrentCulture, "{0:R}", requestDate);
          client.RequestHeaders["authorization"] = authHeader;

          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType));
          this.ETag = client.ResponseHeaders["etag"];

          break;

        case "p":
        case "post":
          method = "POST";

          // accept input doc and parse into valid Atom for Azure Tables
          readBody = ResolveDocument(args[doc]);
          sendBody = string.Format(createEntityXml, requestDate, readBody);
          sendBody = string.Format(sendBody, args[partition], args[row]);
          contentMD5 = h.MD5(sendBody);

          // do POST
          authHeader = CreateSharedKeyAuth(method, canonicalResource, contentMD5, requestDate);
          client.RequestHeaders["Content-MD5"] = contentMD5;
          client.RequestHeaders["x-ms-date"] = string.Format(CultureInfo.CurrentCulture,"{0:R}", requestDate);
          client.RequestHeaders["authorization"] = authHeader;
          
          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType, sendBody));
          Console.Out.WriteLine(string.Format(CultureInfo.CurrentCulture,"Entity [{0}] has been added.", (client.ResponseHeaders["location"] != null ? client.ResponseHeaders["location"] : string.Format(CultureInfo.CurrentCulture,"{0}/{1},{2}", args[table], args[partition], args[row]))));

          break;

        case "u":
        case "put":
          method = "PUT";

          if (args.Length > 2)
          {
            canonicalResource += string.Format(CultureInfo.CurrentCulture, "(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
            requestUrl += string.Format(CultureInfo.CurrentCulture, "(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
          }
          else
          {
            throw new HttpException(400, "Missing PartitionKey and/or RowKey");
          }

          // do GET for Etag;
          authHeader = CreateSharedKeyAuth("GET", canonicalResource, contentMD5, requestDate);
          client.RequestHeaders["x-ms-date"] = string.Format(CultureInfo.CurrentCulture, "{0:R}", requestDate);
          client.RequestHeaders["authorization"] = authHeader;
          readBody = client.Execute(requestUrl, "GET", contentType);
          this.ETag = client.ResponseHeaders["etag"];

          // accept input doc and parse into valid Atom for Azure Tables
          sendBody = ResolveDocument(args[doc]);
          sendBody = string.Format(updateEntityXml, args[table], args[partition], args[row], requestDate, sendBody, this.ETag.Replace(@"""", "&quot;"), this.Account);
          contentMD5 = h.MD5(sendBody);

          // do PUT
          authHeader = CreateSharedKeyAuth(method, canonicalResource, contentMD5, requestDate);
          client.RequestHeaders["x-ms-date"] = string.Format(CultureInfo.CurrentCulture, "{0:R}", requestDate);
          client.RequestHeaders["authorization"] = authHeader;
          client.RequestHeaders["content-md5"] = contentMD5;
          client.RequestHeaders["if-match"] = this.ETag;

          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType, sendBody));
          Console.Out.WriteLine(string.Format(CultureInfo.CurrentCulture,"Entity [{0}] has been replaced.", (client.ResponseHeaders["location"] != null ? client.ResponseHeaders["location"] : string.Format(CultureInfo.CurrentCulture,"{0}/{1},{2}", args[table], args[partition], args[row]))));
          
          break;

        case "m":
        case "merge":
          method = "MERGE";

          if (args.Length > 2)
          {
            canonicalResource += string.Format(CultureInfo.CurrentCulture, "(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
            requestUrl += string.Format(CultureInfo.CurrentCulture, "(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
          }
          else
          {
            throw new HttpException(400, "Missing PartitionKey and/or RowKey");
          }

          // do GET for Etag;
          authHeader = CreateSharedKeyAuth("GET", canonicalResource, contentMD5, requestDate);
          client.RequestHeaders["x-ms-date"] = string.Format(CultureInfo.CurrentCulture, "{0:R}", requestDate);
          client.RequestHeaders["authorization"] = authHeader;
          readBody = client.Execute(requestUrl, "GET", contentType);
          this.ETag = client.ResponseHeaders["etag"];

          // accept input doc and parse into valid Atom for Azure Tables
          sendBody = ResolveDocument(args[doc]);
          sendBody = string.Format(updateEntityXml, args[table], args[partition], args[row], requestDate, sendBody, this.ETag.Replace(@"""", "&quot;"), this.Account);
          contentMD5 = h.MD5(sendBody);

          // do merge
          authHeader = CreateSharedKeyAuth(method, canonicalResource, contentMD5, requestDate);
          client.RequestHeaders["x-ms-date"] = string.Format(CultureInfo.CurrentCulture, "{0:R}", requestDate);
          client.RequestHeaders["authorization"] = authHeader;
          client.RequestHeaders["content-md5"] = contentMD5;
          client.RequestHeaders["if-match"] = this.ETag;

          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType, sendBody));
          Console.Out.WriteLine(string.Format(CultureInfo.CurrentCulture, "Entity [{0}] has been merged.", (client.ResponseHeaders["location"] != null ? client.ResponseHeaders["location"] : string.Format(CultureInfo.CurrentCulture, "{0}/{1},{2}", args[table], args[partition], args[row]))));

          break;

        case "d":
        case "delete":
          method = "DELETE";
          contentMD5 = string.Empty;
          
          // validate uri
          if (args.Length > 2)
          {
            canonicalResource += string.Format(CultureInfo.CurrentCulture, "(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
            requestUrl += string.Format(CultureInfo.CurrentCulture, "(PartitionKey='{0}',RowKey='{1}')", args[partition], args[row]);
          }
          else
          {
            throw new HttpException(400, "Missing PartitionKey and/or RowKey");
          }

          // do GET to recover Etag;
          authHeader = CreateSharedKeyAuth("GET", canonicalResource, contentMD5, requestDate);
          client.RequestHeaders["x-ms-date"] = string.Format(CultureInfo.CurrentCulture, "{0:R}", requestDate);
          client.RequestHeaders["authorization"] = authHeader;
          readBody = client.Execute(requestUrl, "GET", contentType);
          this.ETag = client.ResponseHeaders["etag"];

          // now do DELETE to finish the job
          authHeader = CreateSharedKeyAuth(method, canonicalResource, contentMD5, requestDate);
          client.RequestHeaders["x-ms-date"] = string.Format(CultureInfo.CurrentCulture, "{0:R}", requestDate);
          client.RequestHeaders["authorization"] = authHeader;
          client.RequestHeaders["if-match"] = this.ETag;

          Console.Out.WriteLine(client.Execute(requestUrl, method, contentType));
          Console.Out.WriteLine(string.Format(CultureInfo.CurrentCulture,"Entity [{0}] has been deleted.", (client.ResponseHeaders["location"] != null ? client.ResponseHeaders["location"] : string.Format(CultureInfo.CurrentCulture,"{0}/{1},{2}",args[table],args[partition],args[row]))));

          break;

        default:
          throw new ApplicationException("Invalid Entity Command [" + args[cmd] + "]");
      }
    }

    // cool query parser goes here.
    public void Queries(string[] args)
    {
      int query = 1;
      method = "GET";
      
      // parse query
      string queryText = args[query];
      requestDate = DateTime.UtcNow;
      string[] qparts = queryText.Split('?');
      string canonicalResource = string.Format(CultureInfo.CurrentCulture, "/{0}/{1}", this.Account, qparts[0]);
      string requestUrl = string.Format(CultureInfo.CurrentCulture, "{0}/{1}", this.EndPoint, queryText);

      // do GET
      authHeader = CreateSharedKeyAuth(method, canonicalResource, contentMD5, requestDate);
      client.RequestHeaders["x-ms-date"] = string.Format(CultureInfo.CurrentCulture, "{0:R}", requestDate);
      client.RequestHeaders["authorization"] = authHeader;

      Console.Out.WriteLine(client.Execute(requestUrl, method, contentType));
      this.ETag = client.ResponseHeaders["etag"];

      return;

    }

    // collect properties xml (from disk, if needed)
    private string ResolveDocument(string doc)
    {
      string rtn = string.Empty;

      // passed as literal string?
      if (doc.IndexOf("<m:properties", StringComparison.CurrentCultureIgnoreCase) != -1)
      {
        rtn = doc;
      }
      else
      {
        // must be a file pointer
        if (doc.IndexOf(":", StringComparison.CurrentCultureIgnoreCase) == -1)
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

    private string CreateSharedKeyAuth(string method, string resource, string contentMD5, DateTime requestDate)
    {
      string rtn = string.Empty;
      string fmtHeader = "{0} {1}:{2}";
      string fmtStringToSign = "{0}\n{1}\n{2}\n{3:R}\n{4}";

      string authValue = string.Format(fmtStringToSign, method, contentMD5, contentType, requestDate, resource);
      string sigValue = h.MacSha(authValue, Convert.FromBase64String(this.SharedKey));
      rtn = string.Format(fmtHeader, this.KeyType, this.Account, sigValue);
      
      
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
