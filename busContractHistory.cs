using System;
using System.Data;
using System.Data.OleDb;
using System.Net.Mail;
using System.Text;
using Westwind.BusinessObjects;
using Westwind.Tools;

namespace IOT.AVREFWebWebsite.Business
{
    public class busContractHistory : busDataBase
    {
        public const string StandardFieldList = @"CHF.*";

        private string username = "";

        public string Username
        {
            get { return username; }
            set { username = value; }
        }

        private int nFetchSize = 1000;

        public int FetchSize
        {
            get { return this.nFetchSize; }
            set { this.nFetchSize = value; }
        }
        #region Constructor
        public busContractHistory()
            : base()
        {
            this.Tablename = "CHF";
            this.PkField = "iPrimary";
            this.PkType = PkFieldTypes.intType;
        }

        /// <summary>
        /// Currently used for the Windows Service
        /// </summary>
        /// <param name="strConnectionString"></param>
        public busContractHistory(string strConnectionString)
            : this()
        {
            this.ConnectType = ServerTypes.MySql;
            this.ConnectionString = strConnectionString;
        }
        #endregion

        #region DataSets
        public bool GetEmptyDataSet()
        {
            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM CHF 
                                                      WHERE false", this.SelectFieldsWithUserPreferences(this.Username)));

            return this.ExecuteWithErrorLog(Command, this.Tablename) == 0;

        }

        public bool LoadAllContractHistory()
        {
            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM CHF ", this.SelectFieldsWithUserPreferences(this.Username)));

            if (this.ExecuteWithErrorLog(Command, this.Tablename) < 1)
                this.SetError("Contract History not found.");
            else
            {
                this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
                return true;
            }

            return false;
        }

        public bool LoadContractHistoryByNSN(string strNSN, string strTableName)
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

            string search = $"LIKE CONCAT('{strNSN}', '%')";
            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM CHF 
                                                        WHERE REPLACE(REPLACE(CHF.nsn, '-', ''), ' ', '') ?NSN 
                                                        ORDER BY CHF.date, CHF.contract", this.SelectFieldsWithUserPreferences(this.Username)));
            Command.Parameters.Add(this.CreateParameter("?NSN", search));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
                this.SetError("Contract History not found.");
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                return true;
            }

            return false;
        }

        public bool LoadContractHistoryByNSN(string strNSN)
        {
            return this.LoadContractHistoryByNSN(strNSN, this.DefaultViewName);
        }

        public bool LoadContractHistoryByPartNumber(string strPartNumber, string strTableName)
        {
            StringBuilder sb = new StringBuilder();
            foreach (char cCurrent in strPartNumber)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb.Append(cCurrent);
                }
            }
            strPartNumber = sb.ToString();

            string search = $"LIKE CONCAT('{strPartNumber}', '%')";
            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM CHF 
                                                        WHERE REPLACE(REPLACE(CHF.ref_numb, '-', ''), ' ', '') ?PartNumber 
                                                        ORDER BY CHF.date, CHF.contract", this.SelectFieldsWithUserPreferences(this.Username)));
            Command.Parameters.Add(this.CreateParameter("?PartNumber", search));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
                this.SetError("Contract History not found.");
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                return true;
            }

            return false;
        }

        public bool LoadContractHistoryByPartNumber(string strPartNumber)
        {
            return this.LoadContractHistoryByPartNumber(strPartNumber, this.DefaultViewName);
        }
        #endregion

        #region Functions

        #region Search

        /// <summary>
        /// Searches for and populates a dataset of all ContractHistory data that match the specified value.
        /// </summary>
        /// <param name="valueToFind">The nsn to search for.</param>
        /// <returns>True if one or more results are found</returns>
        public bool SearchContracts(SearchTypes searchType, string valueToFind, bool exactMatch = false)
        {
            exactMatch = false;
            StringBuilder sb = new StringBuilder();
            foreach (char cCurrent in valueToFind)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb.Append(cCurrent);
                }
            }
            valueToFind = sb.ToString();

            string whereClause = String.Empty;
            string fieldExpression;

            switch (searchType)
            {
                case SearchTypes.PartNum:
                    fieldExpression = "REPLACE(REPLACE(CHF.ref_numb_no_dash, '-', ''), ' ', '')";
                    break;

                case SearchTypes.NSN:
                default:
                    fieldExpression = "REPLACE(REPLACE(CHF.NSN, '-', ''), ' ', '')";
                    break;
            }

            string comparisonOperator;

            valueToFind = valueToFind.Replace("-", String.Empty);
            valueToFind = valueToFind.Trim();

            if (exactMatch)
            {
                comparisonOperator = "=";
            }
            else
            {
                comparisonOperator = "LIKE";
                valueToFind += "%";
            }

            whereClause += string.Format("{0} {1} ?valueToFind", fieldExpression, comparisonOperator);

            //if (!valueToFind3.Equals(string.Empty))
            //    whereClause += string.Format(" OR {0} {1} ?valueToFind3", fieldExpression3, comparisonOperator);

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                                    FROM CHF
                                                                    WHERE {1} ORDER BY CHF.NSN, CHF.date, CHF.contract LIMIT {2}", this.SelectFieldsWithUserPreferences(this.Username), whereClause, this.FetchSize.ToString()));

            Command.Parameters.Add(this.CreateParameter("?valueToFind", valueToFind));

            Command.Prepare();

            if (this.ExecuteWithErrorLog(Command, this.Tablename) < 1)
                this.SetError("No Results Found.");
            else
            {
                this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
                return true;
            }

            return false;

        }

        #endregion

        #region Select Fields With User Preferences
        /// <summary>
        /// Adds the appropriate formated fields to the select statement based on the users preferences.
        /// </summary>
        /// <param name="username">Username to retrieve the preferences for</param>
        /// <returns>The list of fields to be selected</returns>
        protected string SelectFieldsWithUserPreferences(string username)
        {
            busUserPreferences oUserPrefs = WebStoreFactory.GetBusUserPreferences();
            oUserPrefs.LoadByUserName(username);

            //Populate fetchsize property
            if (oUserPrefs.DataRow["FetchSize"] != DBNull.Value)
                this.FetchSize = Convert.ToInt32(oUserPrefs.DataRow["FetchSize"]);

            string formatExpression = "CAST({0} AS Decimal(20,2))*" + Convert.ToString(oUserPrefs.DataRow["ConversionFactor"], new System.Globalization.CultureInfo("en-US"));
            string selectFields = busContractHistory.StandardFieldList;

            formatExpression = "ROUND(" + formatExpression + ",2)";

            if ((sbyte)oUserPrefs.DataRow["CurrencyJustification"] == 1)
                formatExpression = "CAST(CONCAT('" + oUserPrefs.DataRow["CurrencySymbol"].ToString().Replace("'", @"\'") + "'," + formatExpression + ") as char(30))";
            else
                formatExpression = "CAST(CONCAT(" + formatExpression + ",'" + oUserPrefs.DataRow["CurrencySymbol"].ToString().Replace("'", @"\'") + "') as char(30))";

            selectFields += "," + string.Format(formatExpression, "CHF.unit_price") + " as formatted_unit_price";
            selectFields += "," + string.Format(formatExpression, "CHF.total") + " as formatted_total";

            return selectFields;
        }


        #endregion

        #endregion
    }
}