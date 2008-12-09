using System;
using System.Text;
using System.Security.Cryptography;
using System.Net;
using System.IO;
using System.Web;
using System.Configuration;

namespace Amundsen.Azure.Storage
{
  /// <summary>
  /// Public Domain 2008 amundsen.com, inc.
  /// @author   mike amundsen (mamund@yahoo.com)
  /// @version  1.0 (2008-12-05)
  /// @notes    test to get list of tables 
  /// </summary>
  class Program
  {
    static void Main(string[] args)
    {
      // args for this sample
      string keyType = "SharedKey";
      string method = "GET";
      string contentMD5 = string.Empty;
      string contentType = "application/atom+xml";
      DateTime reqDate = DateTime.UtcNow;

      // formatters
      string fmtHeader = "{0} {1}:{2}";
      string fmtStringToSign = "{0}\n{1}\n{2}\n{3:R}\n{4}";

      // get data from config file
      string account = GetConfigItem("account");    // your azure project name
      string endPoint = GetConfigItem("endPoint");  // the table endpoint created for your azure project
      string authKey = GetConfigItem("sharedKey");    // the primary key created for your azure project

      // build request/resource strings
      string canonicalResource = string.Format("/{0}/{1}", account, "tables");
      string requestUrl = string.Format("{0}/{1}", endPoint, "tables");

      // build up auth hash
      string authValue = string.Format(fmtStringToSign, method, contentMD5, contentType, reqDate, canonicalResource);
      string sigValue = ComputeMacSha(authValue, Convert.FromBase64String(authKey));
      string authHeader = string.Format(fmtHeader, keyType, account, sigValue);

      // make the request and show response
      try
      {
        string rtn = string.Empty;
        WebRequest req = WebRequest.Create(requestUrl);
        req.Headers.Add("x-ms-date", string.Format("{0:R}", reqDate));
        req.Headers.Add("authorization", authHeader);
        req.ContentType = contentType;
        req.Method = method;

        WebResponse resp = req.GetResponse();
        using (StreamReader sr = new StreamReader(resp.GetResponseStream(), true))
        {
          rtn = sr.ReadToEnd();
          sr.Close();
        }
        resp.Close();
        Console.WriteLine(rtn);
      }
      catch (WebException wex)
      {
        HttpWebResponse wrsp = (HttpWebResponse)wex.Response;
        Console.WriteLine(string.Format("{0} : {1}", wrsp.StatusCode,wrsp.StatusDescription));
      }
      catch (HttpException hex)
      {
        Console.WriteLine(hex.Message);
      }
      catch (Exception ex)
      {
        Console.WriteLine(ex.Message);
      }
      Console.ReadLine();

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

    // hashing helper
    private static string ComputeMacSha(string canonicalizedString, byte[] key)
    {
      byte[] dataToMAC = System.Text.Encoding.UTF8.GetBytes(canonicalizedString);

      using (HMACSHA256 hmacsha1 = new HMACSHA256(key))
      {
        return System.Convert.ToBase64String(hmacsha1.ComputeHash(dataToMAC));
      }
    }

  }

}
