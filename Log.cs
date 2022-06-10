using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataExportJob
{
    class Log
    {
        public static bool info(string strMessage)
        {
            try
            {
                string strFileName = "ConsoleLog";
                //Path.GetTempPath()
                FileStream objFilestream = new FileStream(string.Format("{0}\\{1}", Environment.CurrentDirectory, strFileName), FileMode.Append, FileAccess.Write);
                StreamWriter objStreamWriter = new StreamWriter((Stream)objFilestream);
                objStreamWriter.WriteLine(strMessage);
                objStreamWriter.Close();
                objFilestream.Close();
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        public static bool error(string strMessage, Exception expError)
        {
            try
            {
                string strFileName = "ConsoleLog";
                FileStream objFilestream = new FileStream(string.Format("{0}\\{1}", Environment.CurrentDirectory, strFileName), FileMode.Append, FileAccess.Write);
                StreamWriter writer = new StreamWriter((Stream)objFilestream);
                //  writer.WriteLine(strMessage);
                writer.WriteLine("---------------------------------" + strMessage + "--------------------------------------------");
                writer.WriteLine("Date : " + DateTime.Now.ToString());
                writer.WriteLine();

                while (expError != null)
                {
                    writer.WriteLine(expError.GetType().FullName);
                    writer.WriteLine("Message : " + expError.Message);
                    writer.WriteLine("StackTrace : " + expError.StackTrace);

                    expError = expError.InnerException;
                }
                writer.Close();
                objFilestream.Close();
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
    }
}
