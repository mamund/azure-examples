using System;
using System.Text;
using System.Web;
using System.Net;
using System.IO;
using System.Security.Cryptography;
using System.Configuration;

namespace CreateTable
{
  /// <summary>
  /// Public Domain 2008 amundsen.com, inc.
  /// @author mike amundsen (mamund@yahoo.com)
  /// @version 1.0 (2008-12-06)
  /// @notes:   test to create a table
  /// </summary>
  class Program
  {
    static void Main(string[] args)
    {
      // supply for your account
      string account = GetConfigItem("account");
      string sharedKey = GetConfigItem("sharedKey");
      string urlMask = GetConfigItem("endPoint");
      string newtable = "againtables";

      string endPoint = string.Format(urlMask, account);
      string contentType = "application/atom+xml";
      string keyType = "SharedKey";
      string contentMD5 = string.Empty;
      string fmtHeader = "{0} {1}:{2}";
      string fmtStringToSign = "{0}\n{1}\n{2}\n{3:R}\n{4}";

      string authValue = string.Empty;
      string sigValue = string.Empty;
      string authHeader = string.Empty;
      string method = string.Empty;
      string rtnBody = string.Empty;
      string reqBody = string.Empty;

      string canonicalResource = string.Format("/{0}/{1}", account, "Tables");
      string requestUrl = string.Format("{0}/{1}", endPoint, "Tables");
      DateTime requestDate = DateTime.UtcNow;

      method = "POST";
      reqBody = string.Format(createTableXml, requestDate, newtable);
      contentMD5 = MD5(reqBody);
      authValue = string.Format(fmtStringToSign, method, contentMD5, contentType, requestDate, canonicalResource);
      sigValue = MacSha(authValue, Convert.FromBase64String(sharedKey));
      authHeader = string.Format(fmtHeader, keyType, account, sigValue);

      try
      {
        WebRequest req = WebRequest.Create(requestUrl);
        req.Headers.Add("content-md5", contentMD5);
        req.Headers.Add("x-ms-date", string.Format("{0:R}", requestDate));
        req.Headers.Add("authorization", authHeader);
        req.ContentType = contentType;
        req.ContentLength = reqBody.Length;
        req.Method = method;

        using (StreamWriter sw = new StreamWriter(req.GetRequestStream()))
        {
          sw.Write(reqBody);
          sw.Close();
        }

        WebResponse resp = req.GetResponse();
        using (StreamReader sr = new StreamReader(resp.GetResponseStream(), true))
        {
          rtnBody = sr.ReadToEnd();
          sr.Close();
        }
        Console.WriteLine(rtnBody);
      }
      catch (WebException wex)
      {
        if (wex.Status == WebExceptionStatus.ProtocolError)
        {
          HttpWebResponse wrsp = (HttpWebResponse)wex.Response;
          Console.WriteLine(string.Format("ERROR: {0} : {1}",wrsp.StatusCode,wrsp.StatusDescription));
        }
        else
        {
          Console.WriteLine(string.Format("ERROR: {0}",wex.Message));
        }
      }
    }

    // hashing helper
    static string MacSha(string canonicalizedString, byte[] key)
    {
      byte[] dataToMAC = System.Text.Encoding.UTF8.GetBytes(canonicalizedString);

      using (HMACSHA256 hmacsha1 = new HMACSHA256(key))
      {
        return System.Convert.ToBase64String(hmacsha1.ComputeHash(dataToMAC));
      }
    }

    static string MD5(string data)
    {
      return MD5(data, false);
    }
    static string MD5(string data, bool removeTail)
    {
      string rtn = Convert.ToBase64String(new System.Security.Cryptography.MD5CryptoServiceProvider().ComputeHash(System.Text.Encoding.Default.GetBytes(data)));
      if (removeTail)
        return rtn.Replace("=", "");
      else
        return rtn;
    }

    // config reading helpers
    private static string GetConfigItem(string key)
    {
      return GetConfigItem(key, string.Empty);
    }
    private static string GetConfigItem(string key, string defaultValue)
    {
      return (ConfigurationSettings.AppSettings[key] != null ? ConfigurationSettings.AppSettings[key] : defaultValue);
    }

    static string createTableXml = @"<?xml version=""1.0"" encoding=""utf-8""?>
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

  }
}
