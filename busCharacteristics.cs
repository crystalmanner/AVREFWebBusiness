using System;
using System.Data;
using System.Data.OleDb;
using System.Net.Mail;
using System.Text;
using Westwind.BusinessObjects;
using Westwind.Tools;

namespace IOT.AVREFWebWebsite.Business
{
    public class busCharacteristics : busDataBase
    {

        #region Constructor
        public busCharacteristics()
            : base()
        {
            this.Tablename = "CharGov";
            this.PkField = "iPrimary";
            this.PkType = PkFieldTypes.intType;
        }

        /// <summary>
        /// Currently used for the Windows Service
        /// </summary>
        /// <param name="strConnectionString"></param>
        public busCharacteristics(string strConnectionString)
            : this()
        {
            this.ConnectType = ServerTypes.MySql;
            this.ConnectionString = strConnectionString;
        }
        #endregion

        #region DataSets
        public bool GetEmptyDataSet()
        {
            IDbCommand Command = this.CreateCommand(@"SELECT CharGov.* 
                                                      FROM CharGov 
                                                      WHERE false");

            return this.ExecuteWithErrorLog(Command, this.Tablename) == 0;

        }

        public bool LoadAllCharacteristics()
        {
            IDbCommand Command = this.CreateCommand(@"SELECT CharGov.* 
                                                      FROM CharGov ");

            if (this.ExecuteWithErrorLog(Command, this.Tablename) < 1)
                this.SetError("Characteristic not found.");
            else
            {
                this.AggregateText(this.DataSet.Tables[this.Tablename]);
                this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
                return true;
            }

            return false;
        }

        public bool LoadCharacteristicsByNSN(string strNSN, string strTableName)
        {
            StringBuilder sb = new StringBuilder();
            foreach (char cCurrent in strNSN)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb.Append(cCurrent);
                }
            }
            strNSN = sb.ToString();

            string fsc = "", niin = "";
            if (strNSN.Length > 4)
            {
                fsc = strNSN.Substring(0, 4);
                niin = strNSN.Substring(4);
                niin += "%";
            }
            else
                fsc = strNSN;

            fsc += "%";

            IDbCommand Command = this.CreateCommand(@"SELECT CharGov.* 
                                                      FROM CharGov 
                                                        WHERE CharGov.fsc LIKE ?FSC AND CharGov.niin LIKE ?NIIN");
            Command.Parameters.Add(this.CreateParameter("?FSC", fsc));
            Command.Parameters.Add(this.CreateParameter("?NIIN", niin));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
                this.SetError("Contract History not found.");
            else
            {
                this.AggregateText(this.DataSet.Tables[strTableName]);
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                return true;
            }

            return false;
        }
        public bool LoadCharacteristicsByNSN(string strNSN)
        {
            return this.LoadCharacteristicsByNSN(strNSN, this.DefaultViewName);
        }
        #endregion

        #region Functions
        private void AggregateText(DataTable dt)
        {
            dt.Columns["required"].MaxLength = int.MaxValue;
            dt.Columns["decoded"].MaxLength = int.MaxValue;

            DataRow drMain=null;

            foreach (DataRow dr in dt.Rows)
            {
                if (drMain != null && (dr["fsc"].ToString().Equals(drMain["fsc"].ToString()) && dr["niin"].ToString().Equals(drMain["niin"].ToString()) && dr["mrc"].ToString().Equals(drMain["mrc"].ToString())))
                {
                    if (dr["required"].ToString().Equals(string.Empty))
                    {
                        drMain["decoded"] = drMain["decoded"].ToString() + " " + dr["decoded"].ToString();
                        dr.Delete();
                    }
                    else if (dr["decoded"].ToString().Equals(string.Empty))
                    {
                        drMain["required"] = drMain["required"].ToString() + " " + dr["required"].ToString();
                        dr.Delete();
                    }
                    else
                        drMain = dr;
                }
                else
                    drMain = dr;
            }

            dt.AcceptChanges();
        }
        #endregion
    }
}