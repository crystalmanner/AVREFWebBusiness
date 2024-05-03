using System;
using System.Collections;
using System.Data;
using System.Data.OleDb;
using System.Net.Mail;
using System.Text;
using Westwind.BusinessObjects;
using Westwind.Tools;

namespace IOT.AVREFWebWebsite.Business
{
    public class busExportCodes : busDataBase
    {
        //private string username = "";

        //public string Username
        //{
        //    get { return username; }
        //    set { username = value; }
        //}

        //private int nFetchSize = 100;

        //public int FetchSize
        //{
        //    get { return this.nFetchSize; }
        //    set { this.nFetchSize = value; }
        //}

        #region Constructor
        public busExportCodes()
            : base()
        {
            this.Tablename = "ExportCodes";
            this.PkField = "p_sysID";
            this.PkType = PkFieldTypes.intType;
        }

        /// <summary>
        /// Currently used by the Windows service
        /// </summary>
        /// <param name="strConnectionString"></param>
        public busExportCodes(string strConnectionString)
            : this()
        {
            this.ConnectType = ServerTypes.MySql;
            this.ConnectionString = strConnectionString;
        }
        #endregion

        #region DataSets

        #region Load Parts
        public bool GetEmptyDataSet()
        {
            IDbCommand Command = this.CreateCommand(@"SELECT ExportCodes.*                                                      
                                                      FROM ExportCodes
                                                      WHERE ExportCodes.p_sysID = -1");

            return this.ExecuteWithErrorLog(Command, this.Tablename) == 0;

        }

        public bool LoadByPartID(int nPartID, string strTableName)
        {
            bool retVal = false;

            IDbCommand Command = this.CreateCommand(@"SELECT ExportCodes.*                                                      
                                                      FROM ExportCodes
                                                      WHERE ExportCodes.p_sysID = ?PartID");
            Command.Parameters.Add(this.CreateParameter("?PartID", nPartID));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("Export code not found.");
                retVal = false;
            }
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                retVal = true;
            }

            return retVal;
        }
        public bool LoadByPartID(int nPartID)
        {
            return this.LoadByPartID(nPartID, this.DefaultViewName);
        }

        #endregion

        #endregion

        #region Functions
        
        #region Select Fields With User Preferences
        ///// <summary>
        ///// Adds the appropriate formated fields to the select statement based on the users preferences.
        ///// </summary>
        ///// <param name="username">Username to retrieve the preferences for</param>
        ///// <returns>The list of fields to be selected</returns>
        //protected string SelectFieldsWithUserPreferences(string username)
        //{
        //    busUserPreferences oUserPrefs = WebStoreFactory.GetBusUserPreferences();
        //    oUserPrefs.LoadByUserName(username);

        //    //Populate fetchsize property
        //    if (oUserPrefs.DataRow["FetchSize"] != DBNull.Value)
        //        this.FetchSize = Convert.ToInt32(oUserPrefs.DataRow["FetchSize"]);

        //    string formatExpression = "CAST({0} AS Decimal(20,2))*" + Convert.ToString(oUserPrefs.DataRow["ConversionFactor"], new System.Globalization.CultureInfo("en-US"));
        //    string selectFields = busParts.StandardFieldList;

        //    formatExpression = "ROUND(" + formatExpression + ",2)";

        //    if ((sbyte)oUserPrefs.DataRow["CurrencyJustification"] == 1)
        //        formatExpression = "CAST(CONCAT('" + oUserPrefs.DataRow["CurrencySymbol"].ToString().Replace("'", @"\'") + "'," + formatExpression + ") as char(30))";
        //    else
        //        formatExpression = "CAST(CONCAT(" + formatExpression + ",'" + oUserPrefs.DataRow["CurrencySymbol"].ToString().Replace("'", @"\'") + "') as char(30))";

        //    selectFields += "," + string.Format(formatExpression, "PartInfo.p_price") + " as formatted_p_price";
        //    selectFields += "," + string.Format(formatExpression, "PartInfo.p_deposit") + " as formatted_p_deposit";
        //    selectFields += "," + string.Format(formatExpression, "PartInfo.price2") + " as formatted_price2";
        //    selectFields += "," + string.Format(formatExpression, "CAST(CAST(PartInfo.p_price as Decimal(20,2)) * (1 - COALESCE(" + busParts.MetaDatabase + ".Discounts.d_percent, 0) / 100) as Decimal(20,2))") + " as formatted_discounted_price";


        //    return selectFields;
        //}


        #endregion

        #endregion
    }
}