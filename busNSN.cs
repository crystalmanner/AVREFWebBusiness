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
    public class busNSN : busDataBase
    {
        #region Constants
        public const string MDISInformationMessage = "No Results Found In MCRL, Below Are The Results For MDIS Records";
        public const string CHFInformationMessage = "No Results Found In MCRL or MDIS, Below Are The Results For CHF Records";
        public const string CHARInformationMessage = "No Results Found In MCRL, MDIS, or CHF, Below Are The Results For CHAR Records";
        public const string StandardFieldList = @"MCRL.cage_cd_92, 
                                                    MCRL.can_nsn, 
                                                    MCRL.dup_da, 
                                                    MCRL.dup_isc, 
                                                    MCRL.enac, 
                                                    MCRL.esdc,
                                                    MCRL.fsc, 
                                                    MCRL.hcc, 
                                                    MCRL.hmic, 
                                                    MCRL.inc_code, 
                                                    COALESCE(IF(mdis.item_nam='',null,mdis.item_nam),mcrl.item_nam) as item_nam, 
                                                    MCRL.msds, 
                                                    MCRL.niin, 
                                                    MCRL.ref_numb, 
                                                    MCRL.rncc, 
                                                    MCRL.rnvc, 
                                                    MCRL.sadc, 
                                                    MCRL.tLastUpdt, 
                                                    MCRL.iPrimary, 
                                                    CONCAT(MCRL.fsc,MCRL.NIIN) as NSN,
                                                    MDIS.pmic, 
                                                    MDIS.critcl_cd, 
                                                    MDIS.adp, 
                                                    MDIS.edc_code, 
                                                    MDIS.hmic_code, 
                                                    MDIS.demil, 
                                                    MDIS.sa, 
                                                    MDIS.sos, 
                                                    MDIS.aac, 
                                                    MDIS.qup, 
                                                    MDIS.ui, 
                                                    MDIS.price, 
                                                    MDIS.slc, 
                                                    MDIS.ciic, 
                                                    MDIS.rep, 
                                                    MDIS.mgmnt_cds, 
                                                    MDIS.phrase_com, 
                                                    MCRL.iPrimary as MCRLID,
                                                    null as cage, 
                                                    null as contract,
                                                    null as qty,
                                                    null as uom,
                                                    null as formatted_unit_price,
                                                    null as formatted_total,
                                                    CurDate()    as date,
                                                    COALESCE(MDIS.iPrimary,0) as MDISID";
        public const string StandardFromClause = @"MCRL               
                                                LEFT JOIN MDIS On MCRL.fsc=MDIS.fsc AND MCRL.niin=MDIS.niin AND MDIS.tLastUpdt is not NULL";

        public const string StandardMDISFieldList = @"null as cage_cd_92, 
                                                    null as can_nsn, 
                                                    null as dup_da, 
                                                    null as dup_isc, 
                                                    null as enac, 
                                                    null as esdc,
                                                    MDIS.fsc, 
                                                    null as hcc, 
                                                    null as hmic, 
                                                    null as inc_code, 
                                                    IF(mdis.item_nam='',null,mdis.item_nam) as item_nam, 
                                                    null as msds, 
                                                    MDIS.niin, 
                                                    null as ref_numb, 
                                                    null as rncc, 
                                                    null as rnvc, 
                                                    null as sadc, 
                                                    null as tLastUpdt, 
                                                    null as iPrimary, 
                                                    CONCAT(MDIS.fsc,MDIS.NIIN) as NSN, 
                                                    MDIS.pmic, 
                                                    MDIS.critcl_cd, 
                                                    MDIS.adp, 
                                                    MDIS.edc_code, 
                                                    MDIS.hmic_code, 
                                                    MDIS.demil, 
                                                    MDIS.sa, 
                                                    MDIS.sos, 
                                                    MDIS.aac, 
                                                    MDIS.qup, 
                                                    MDIS.ui, 
                                                    MDIS.price, 
                                                    MDIS.slc, 
                                                    MDIS.ciic, 
                                                    MDIS.rep, 
                                                    MDIS.mgmnt_cds, 
                                                    MDIS.phrase_com, 
                                                    null as MCRLID, 
                                                    null as cage,
                                                    null as contract,
                                                    null as qty,
                                                    null as uom,
                                                    null as formatted_unit_price,
                                                    null as formatted_total,
                                                    CurDate() as date,
                                                    COALESCE(MDIS.iPrimary,0) as MDISID";

        public const string StandardMDISFromClause = @"MDIS";

        public const string StandardCHFFieldList = @"CHF.*,
                                                    null as cage_cd_92, 
                                                    null as can_nsn, 
                                                    null as dup_da, 
                                                    null as dup_isc, 
                                                    null as enac, 
                                                    null as esdc,
                                                    null as fsc, 
                                                    null as hcc, 
                                                    null as hmic, 
                                                    null as inc_code, 
                                                    null as item_nam, 
                                                    null as msds, 
                                                    null as niin, 
                                                    null as ref_numb, 
                                                    null as rncc, 
                                                    null as rnvc, 
                                                    null as sadc, 
                                                    null as pmic, 
                                                    null as critcl_cd, 
                                                    null as adp, 
                                                    null as edc_code, 
                                                    null as hmic_code, 
                                                    null as demil, 
                                                    null as sa, 
                                                    null as sos, 
                                                    null as aac, 
                                                    null as qup, 
                                                    null as ui, 
                                                    null as price, 
                                                    null as slc, 
                                                    null as ciic, 
                                                    null as rep, 
                                                    null as mgmnt_cds, 
                                                    null as phrase_com, 
                                                    null as MCRLID, 
                                                    null as MDISID";

        public const string StandardCHFFromClause = @"CHF";

        public const string StandardCHARFieldList = @"null as cage_cd_92, 
                                                    null as can_nsn, 
                                                    null as dup_da, 
                                                    null as dup_isc, 
                                                    null as enac, 
                                                    null as esdc,
                                                    CharGov.fsc, 
                                                    null as hcc, 
                                                    null as hmic, 
                                                    null as inc_code, 
                                                    CharGov.item_name as item_nam, 
                                                    null as msds, 
                                                    CharGov.niin, 
                                                    null as ref_numb, 
                                                    null as rncc, 
                                                    null as rnvc, 
                                                    null as sadc, 
                                                    null as tLastUpdt, 
                                                    null as iPrimary, 
                                                    CONCAT(CharGov.fsc,CharGov.NIIN) as NSN, 
                                                    null as pmic, 
                                                    null as critcl_cd, 
                                                    null as adp, 
                                                    null as edc_code, 
                                                    null as hmic_code, 
                                                    null as demil, 
                                                    null as sa, 
                                                    null as sos, 
                                                    null as aac, 
                                                    null as qup, 
                                                    null as ui, 
                                                    null as price, 
                                                    null as slc, 
                                                    null as ciic, 
                                                    null as rep, 
                                                    null as mgmnt_cds, 
                                                    null as phrase_com, 
                                                    null as MCRLID, 
                                                    null as cage,
                                                    null as contract,
                                                    null as qty,
                                                    null as uom,
                                                    null as formatted_unit_price,
                                                    null as formatted_total,
                                                    CurDate() as date,
                                                    null as MDISID";

        public const string StandardCHARFromClause = @"CharGov";
        #endregion

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

        public enum EnumEnhancedSearchResultsType
        {
            NONE,
            MDIS,
            CHAR,
            CHF
        }

        private EnumEnhancedSearchResultsType _eEnhancedSearchResultsType = EnumEnhancedSearchResultsType.NONE;
        public EnumEnhancedSearchResultsType eEnhancedSearchResultsType
        {
            get
            {
                return this._eEnhancedSearchResultsType;
            }
            set
            {
                this._eEnhancedSearchResultsType = value;
            }
        }

        #region Constructor
        public busNSN()
            : base()
        {
            this.Tablename = "MCRL";
            this.PkField = "iPrimary";
            this.PkType = PkFieldTypes.intType;
        }

        /// <summary>
        /// Currently used by the Windows service
        /// </summary>
        /// <param name="strConnectionString"></param>
        public busNSN(string strConnectionString)
            : this()
        {
            this.ConnectType = ServerTypes.MySql;
            this.ConnectionString = strConnectionString;
        }
        #endregion

        #region DataSets

        #region Load Data
        public bool GetEmptyDataSet()
        {
            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}                                                      
                                                      FROM {1}
                                                      WHERE false", this.SelectFieldsWithUserPreferences(this.Username), busNSN.StandardFromClause));

            return this.ExecuteWithErrorLog(Command, this.Tablename) == 0;

        }

        public bool LoadByNSN(string strNSN, string strTableName)
        {
            bool retVal = false;
            string fsc = "", niin = "";
            StringBuilder sb = new StringBuilder();
            foreach (char cCurrent in strNSN)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb.Append(cCurrent);
                }
            }
            strNSN = sb.ToString();

            if (strNSN.Length > 4)
            {
                fsc = strNSN.Substring(0, 4);
                niin = strNSN.Substring(4);
            }
            else
                fsc = strNSN;

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM {1}                 
                                                      WHERE MCRL.fsc=?FSC AND MCRL.niin=?NIIN AND MCRL.tLastUpdt is not NULL", this.SelectFieldsWithUserPreferences(this.Username), busNSN.StandardFromClause));
            Command.Parameters.Add(this.CreateParameter("?FSC", fsc));
            Command.Parameters.Add(this.CreateParameter("?NIIN", niin));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("NSN not found.");
                retVal = false;
            }
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                retVal = true;
            }

            return retVal;
        }
        public bool LoadByNSN(string strNSN)
        {
            return this.LoadByNSN(strNSN, this.DefaultViewName);
        }

        public bool LoadByNSNAndPartNumber(string strNSN, string strPartNum, string strTableName)
        {
            bool retVal = false;
            string fsc = "", niin = "";

            StringBuilder sb = new StringBuilder();
            foreach (char cCurrent in strNSN)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb.Append(cCurrent);
                }
            }
            strNSN = sb.ToString();

            if (strNSN.Length > 4)
            {
                fsc = strNSN.Substring(0, 4);
                niin = strNSN.Substring(4);
            }
            else
                fsc = strNSN;

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM {1}                 
                                                      WHERE MCRL.fsc=?FSC AND MCRL.niin=?NIIN AND MCRL.ref_numb=?PartNumber AND MCRL.tLastUpdt is not NULL", this.SelectFieldsWithUserPreferences(this.Username), busNSN.StandardFromClause));
            Command.Parameters.Add(this.CreateParameter("?FSC", fsc));
            Command.Parameters.Add(this.CreateParameter("?NIIN", niin));
            Command.Parameters.Add(this.CreateParameter("?PartNumber", strPartNum));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("NSN not found.");
                retVal = false;
            }
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                retVal = true;
            }

            return retVal;
        }
        public bool LoadByNSNAndPartNumber(string strNSN, string strPartNum)
        {
            return this.LoadByNSNAndPartNumber(strNSN, strPartNum, this.DefaultViewName);
        }

        public bool LoadByNSNPartNumberAndCage(string strNSN, string strPartNum, string strCage, string strTableName)
        {
            bool retVal = false;
            string fsc = "", niin = "";

            StringBuilder sb = new StringBuilder();
            foreach (char cCurrent in strNSN)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb.Append(cCurrent);
                }
            }
            strNSN = sb.ToString();

            if (strNSN.Length > 4)
            {
                fsc = strNSN.Substring(0, 4);
                niin = strNSN.Substring(4);
            }
            else
                fsc = strNSN;

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM {1}                 
                                                      WHERE MCRL.fsc=?FSC AND MCRL.niin=?NIIN AND MCRL.ref_numb=?PartNumber AND MCRL.cage_cd_92=?Cage AND MCRL.tLastUpdt is not NULL", this.SelectFieldsWithUserPreferences(this.Username), busNSN.StandardFromClause));
            Command.Parameters.Add(this.CreateParameter("?FSC", fsc));
            Command.Parameters.Add(this.CreateParameter("?NIIN", niin));
            Command.Parameters.Add(this.CreateParameter("?PartNumber", strPartNum));
            Command.Parameters.Add(this.CreateParameter("?Cage", strCage));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("NSN not found.");
                retVal = false;
            }
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                retVal = true;
            }

            return retVal;
        }
        public bool LoadByNSNPartNumberAndCage(string strNSN, string strPartNum, string strCage)
        {
            return this.LoadByNSNPartNumberAndCage(strNSN, strPartNum, strCage, this.DefaultViewName);
        }

        public bool LoadByNSNAndCage(string strNSN, string strCage, string strTableName)
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

            StringBuilder sb2 = new StringBuilder();
            foreach (char cCurrent in strCage)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb2.Append(cCurrent);
                }
            }
            strCage = sb2.ToString();

            bool retVal = false;
            string fsc = "", niin = "";
            if (strNSN.Length > 4)
            {
                fsc = strNSN.Substring(0, 4);
                niin = strNSN.Substring(4);
            }
            else
                fsc = strNSN;

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM {1}                 
                                                      WHERE MCRL.fsc=?FSC AND MCRL.niin=?NIIN AND MCRL.cage_cd_92=?Cage AND MCRL.tLastUpdt is not NULL", this.SelectFieldsWithUserPreferences(this.Username), busNSN.StandardFromClause));
            Command.Parameters.Add(this.CreateParameter("?FSC", fsc));
            Command.Parameters.Add(this.CreateParameter("?NIIN", niin));
            Command.Parameters.Add(this.CreateParameter("?Cage", strCage));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("NSN not found.");
                retVal = false;
            }
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                retVal = true;
            }

            return retVal;
        }
        public bool LoadByNSNAndCage(string strNSN, string strCage)
        {
            return this.LoadByNSNAndCage(strNSN, strCage, this.DefaultViewName);
        }

        public bool LoadCHFByNSN(string strNSN, string strTableName)
        {
            bool retVal = false;

            StringBuilder sb = new StringBuilder();
            foreach (char cCurrent in strNSN)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb.Append(cCurrent);
                }
            }
            strNSN = sb.ToString();

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM {1}                 
                                                      WHERE CHF.NSN=?NSN", this.SelectFieldsWithUserPreferences(this.Username, busNSN.EnumEnhancedSearchResultsType.CHF), busNSN.StandardCHFFromClause));
            Command.Parameters.Add(this.CreateParameter("?NSN", strNSN));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("NSN not found.");
                retVal = false;
            }
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                retVal = true;
            }

            return retVal;
        }
        public bool LoadCHFByNSN(string strNSN)
        {
            return this.LoadCHFByNSN(strNSN, this.DefaultViewName);
        }

        public bool LoadCHARByNSN(string strNSN, string strTableName)
        {
            bool retVal = false;
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
            }
            else
                fsc = strNSN;

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM {1}                 
                                                      WHERE CHARGOV.fsc=?FSC AND CHARGOV.niin=?NIIN", this.SelectFieldsWithUserPreferences(this.Username, busNSN.EnumEnhancedSearchResultsType.CHAR), busNSN.StandardCHARFromClause));
            Command.Parameters.Add(this.CreateParameter("?FSC", fsc));
            Command.Parameters.Add(this.CreateParameter("?NIIN", niin));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("NSN not found.");
                retVal = false;
            }
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                retVal = true;
            }

            return retVal;
        }
        public bool LoadCHARByNSN(string strNSN)
        {
            return this.LoadCHARByNSN(strNSN, this.DefaultViewName);
        }

        public bool LoadByMDIS(string strMDIS, string strTableName)
        {
            bool retVal = false;
            int MDISID = 0;

            StringBuilder sb = new StringBuilder();
            foreach (char cCurrent in strMDIS)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb.Append(cCurrent);
                }
            }
            strMDIS = sb.ToString();

            int.TryParse(strMDIS, out MDISID);

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM {1}                 
                                                      WHERE MDIS.iPrimary =?MDISID", this.SelectFieldsWithUserPreferences(this.Username, EnumEnhancedSearchResultsType.MDIS)
                                                                                                                             , busNSN.StandardMDISFromClause));
            Command.Parameters.Add(this.CreateParameter("?MDISID", MDISID));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("NSN not found.");
                retVal = false;
            }
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                retVal = true;
            }

            return retVal;
        }
        public bool LoadByMDIS(string strMDISID)
        {
            return this.LoadByMDIS(strMDISID, this.DefaultViewName);
        }

        public bool LoadByIDs(string strMCRLID, string strMDISID, string strTableName)
        {
            bool retVal = false;
            int MCRLID = 0, MDISID = 0;
            int.TryParse(strMCRLID, out MCRLID);
            int.TryParse(strMDISID, out MDISID);

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM {1}                 
                                                      WHERE MCRL.iPrimary=?MCRLID AND (MDIS.iPrimary=?MDISID OR MDIS.iPrimary is null) AND MCRL.tLastUpdt is not NULL", this.SelectFieldsWithUserPreferences(this.Username), busNSN.StandardFromClause));
            Command.Parameters.Add(this.CreateParameter("?MCRLID", MCRLID));
            Command.Parameters.Add(this.CreateParameter("?MDISID", MDISID));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("NSN not found.");
                retVal = false;
            }
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                retVal = true;
            }

            return retVal;
        }
        public bool LoadByIDs(string strMCRLID, string strMDISID)
        {
            return this.LoadByIDs(strMCRLID, strMDISID, this.DefaultViewName);
        }

        public bool LoadAllNSN()
        {
            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                      FROM {1}
                                                        WHERE MCRL.tLastUpdt is not NULL", this.SelectFieldsWithUserPreferences(this.Username), busNSN.StandardFromClause));

            if (this.ExecuteWithErrorLog(Command, this.Tablename) < 1)
                this.SetError("NSN not found.");
            else
            {
                this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
                return true;
            }

            return false;
        }

        #endregion

        #endregion

        #region Functions

        #region Search
        /// <summary>
        /// Searches for and populates a dataset of all MCRL data that match the specified value.
        /// </summary>
        /// <param name="valueToFind">The ref/part number,nsn, or cage to search for.</param>
        /// <param name="typeOfSearch">The type of search to perform the search on. See <see cref="SearchTypes.cs">SearchTypes.cs</see> for a list of available types.</param>
        /// <param name="removeHyphens">Indicates if the search should be performed with hyphens removed.</param>
        /// <returns>True if one or more results are found</returns>
        public bool SearchMCRL(string valueToFind, SearchTypes typeOfSearch, bool exactMatch = false, bool removeHyphens = true)
        {
            string whereClause = String.Empty;
            string fieldExpression, fieldExpression2 = "";
            string valueToFind2 = "";
            string comparisonOperator;
            string strNSNPartResults = String.Empty;
            string strOriginalResultsOrder = String.Empty;

            exactMatch = false;
            removeHyphens = true;

            StringBuilder sb = new StringBuilder();
            foreach (char cCurrent in valueToFind)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb.Append(cCurrent);
                }
            }
            valueToFind = sb.ToString();

            switch (typeOfSearch)
            {
                case SearchTypes.NSN:
                    // fieldExpression = "MCRL.fsc";
                    // fieldExpression2 = "MCRL.niin";
                    fieldExpression = "REPLACE(REPLACE(MCRL.fsc, '-', ''), ' ', '')";
                    fieldExpression2 = "REPLACE(REPLACE(MCRL.niin, '-', ''), ' ', '')";

                    //fieldExpression3 = "MCRL.can_nsn";
                    //valueToFind3 = valueToFind;
                    valueToFind = valueToFind.Replace("-", String.Empty);
                    if (valueToFind.Length > 4)
                    {
                        valueToFind2 = valueToFind.Substring(4);
                        valueToFind = valueToFind.Substring(0, 4);
                    }
                    break;
                case SearchTypes.CAGE:
                    // fieldExpression = "MCRL.Cage_cd_92";
                    fieldExpression = "REPLACE(REPLACE(MCRL.Cage_cd_92, '-', ''), ' ', '')";
                    break;

                case SearchTypes.Description:
                    fieldExpression = "REPLACE(REPLACE(COALESCE(IF(mdis.item_nam='',null,mdis.item_nam),mcrl.item_nam), '-', ''), ' ', '')";
                    break;

                case SearchTypes.PartNum:
                default:
                    //08-09-2009: Always removing hyphens unless exact match (still leaving it as a parameter incase mind is changed)
                    if (!exactMatch || removeHyphens)
                    {
                        // fieldExpression = "MCRL.ref_numb_no_dash";
                        fieldExpression = "REPLACE(REPLACE(MCRL.ref_numb_no_dash, '-', ''), ' ', '')";

                        foreach (char cCurrent in valueToFind)
                        {
                            if (!char.IsLetterOrDigit(cCurrent))
                                valueToFind = valueToFind.Replace(cCurrent.ToString(), String.Empty).Trim();
                        }

                        //valueToFind = valueToFind.Replace("-", "");
                    }
                    else
                        // fieldExpression = "MCRL.ref_numb";
                        fieldExpression = "REPLACE(REPLACE(MCRL.ref_numb, '-', ''), ' ', '')";

                    break;
            }


            valueToFind = valueToFind.Trim();
            valueToFind2 = valueToFind2.Trim();
            //valueToFind3 = valueToFind3.Trim();

            if (exactMatch)
            {
                comparisonOperator = "=";
            }
            else
            {
                comparisonOperator = "LIKE";
                if (valueToFind2.Equals(string.Empty))
                {
                    if (typeOfSearch == SearchTypes.Description)
                    {
                        if (!string.IsNullOrEmpty(valueToFind))
                        {
                            var individualWords = valueToFind.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

                            valueToFind = "+" + string.Join("* +", individualWords) + "*";
                        }
                    }
                    else
                        valueToFind += "%";
                }
                else
                    valueToFind2 += "%";

                //if(!valueToFind3.Equals(String.Empty))
                //    valueToFind3 += "%";
            }

            if (typeOfSearch == SearchTypes.Description && !exactMatch)
                whereClause += "((mdis.item_nam IS NOT NULL AND MATCH(mdis.item_nam) AGAINST (?valueToFind in boolean mode)) OR (mcrl.item_nam IS NOT NULL AND MATCH(mcrl.item_nam) AGAINST (?valueToFind in boolean mode)))";
            else
                whereClause += string.Format("{0} {1} ?valueToFind", fieldExpression, comparisonOperator);

            if (!valueToFind2.Equals(string.Empty))
                whereClause += string.Format(" AND {0} {1} ?valueToFind2", fieldExpression2, comparisonOperator);

            //if (!valueToFind3.Equals(string.Empty))
            //    whereClause += string.Format(" OR {0} {1} ?valueToFind3", fieldExpression3, comparisonOperator);

            IDbCommand Command;
            var codeFields = "NsnExportCodes.SchbCode";
            var codeJoin = "LEFT JOIN NsnExportCodes ON NsnExportCodes.P_NSN = CONCAT(MCRL.fsc,MCRL.NIIN)";
            if (typeOfSearch != SearchTypes.PartNum)
            {
                Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                                    FROM {1}
                                                                    WHERE {2} LIMIT {3}",
                                                                    $"{this.SelectFieldsWithUserPreferences(this.Username)}, {codeFields}",
                                                                    $"{busNSN.StandardFromClause} {codeJoin}",
                                                                    whereClause,
                                                                    this.FetchSize.ToString()));
            }
            else
            {
                Command = this.CreateCommand(string.Format(@"SELECT MCRL.iPrimary,CONCAT(MCRL.fsc,MCRL.NIIN) as NSN
                                                                    FROM {1}
                                                                    WHERE {2} LIMIT {3}",
                                                                    $"{this.SelectFieldsWithUserPreferences(this.Username)}, {codeFields}",
                                                                    $"{busNSN.StandardFromClause} {codeJoin}",
                                                                    whereClause,
                                                                    this.FetchSize.ToString()));
            }

            Command.Parameters.Add(this.CreateParameter("?valueToFind", valueToFind));
            if (!valueToFind2.Equals(string.Empty))
                Command.Parameters.Add(this.CreateParameter("?valueToFind2", valueToFind2));
            //if (!valueToFind3.Equals(string.Empty))
            //    Command.Parameters.Add(this.CreateParameter("?valueToFind3", valueToFind3));

            Command.Prepare();

            if (this.ExecuteWithErrorLog(Command, this.Tablename) < 1)
            {
                if (typeOfSearch == SearchTypes.NSN)
                {
                    this.eEnhancedSearchResultsType = EnumEnhancedSearchResultsType.MDIS;
                    // fieldExpression = "MDIS.fsc";
                    // fieldExpression2 = "MDIS.niin";
                    fieldExpression = "REPLACE(REPLACE(MDIS.fsc, '-', ''), ' ', '')";
                    fieldExpression2 = "REPLACE(REPLACE(MDIS.niin, '-', ''), ' ', '')";

                    whereClause = string.Format("{0} {1} ?valueToFind", fieldExpression, comparisonOperator);

                    if (!valueToFind2.Equals(string.Empty))
                        whereClause += string.Format(" AND {0} {1} ?valueToFind2", fieldExpression2, comparisonOperator);

                    //Try to search MDIS First, Then CHF THEN Char
                    Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                                 FROM {1}
                                                                 WHERE {2}
                                                                 LIMIT {3}",
                                                                 $"{busNSN.StandardMDISFieldList}, {codeFields}",
                                                                 $"{busNSN.StandardMDISFromClause} LEFT JOIN NsnExportCodes ON NsnExportCodes.P_NSN = CONCAT(MDIS.fsc, MDIS.NIIN)",
                                                                 whereClause,
                                                                 this.FetchSize.ToString()));

                    Command.Parameters.Add(this.CreateParameter("?valueToFind", valueToFind));
                    if (!valueToFind2.Equals(string.Empty))
                        Command.Parameters.Add(this.CreateParameter("?valueToFind2", valueToFind2));

                    Command.Prepare();

                    if (this.ExecuteWithErrorLog(Command, this.Tablename) < 1)
                    {
                        //CharGov Table Where Clause
                        this.eEnhancedSearchResultsType = EnumEnhancedSearchResultsType.CHAR;
                        // fieldExpression = "CharGov.fsc";
                        // fieldExpression2 = "CharGov.niin";
                        fieldExpression = "REPLACE(REPLACE(CharGov.fsc, '-', ''), ' ', '')";
                        fieldExpression2 = "REPLACE(REPLACE(CharGov.niin, '-', ''), ' ', '')";

                        whereClause = string.Format("{0} {1} ?valueToFind", fieldExpression, comparisonOperator);

                        if (!valueToFind2.Equals(string.Empty))
                            whereClause += string.Format(" AND {0} {1} ?valueToFind2", fieldExpression2, comparisonOperator);

                        Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                                     FROM {1}
                                                                     WHERE {2}
                                                                     LIMIT {3}",
                                                                     $"{busNSN.StandardCHARFieldList}, {codeFields}",
                                                                     $"{busNSN.StandardCHARFromClause} LEFT JOIN NsnExportCodes ON NsnExportCodes.P_NSN = CONCAT(CharGov.fsc, CharGov.NIIN)",
                                                                     whereClause,
                                                                     this.FetchSize.ToString()));

                        Command.Parameters.Add(this.CreateParameter("?valueToFind", valueToFind));
                        if (!valueToFind2.Equals(string.Empty))
                            Command.Parameters.Add(this.CreateParameter("?valueToFind2", valueToFind2));

                        Command.Prepare();

                        if (this.ExecuteWithErrorLog(Command, this.Tablename) < 1)
                        {
                            //CHF Table Where Clause
                            this.eEnhancedSearchResultsType = EnumEnhancedSearchResultsType.CHF;
                            // fieldExpression = "CHF.NSN";
                            fieldExpression = "REPLACE(REPLACE(CHF.NSN, '-', ''), ' ', '')";

                            whereClause = string.Format("{0} {1} ?valueToFind", fieldExpression, comparisonOperator);

                            Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                                     FROM {1}
                                                                     WHERE {2}
                                                                     LIMIT {3}",
                                                                     $"{this.SelectFieldsWithUserPreferences(this.Username, EnumEnhancedSearchResultsType.CHF)}, {codeFields}",
                                                                     $"{busNSN.StandardCHFFromClause} LEFT JOIN NsnExportCodes ON CHF.NSN = NsnExportCodes.P_NSN",
                                                                     whereClause,
                                                                     this.FetchSize.ToString()));


                            Command.Parameters.Add(this.CreateParameter("?valueToFind", valueToFind + valueToFind2));

                            Command.Prepare();

                            if (this.ExecuteWithErrorLog(Command, this.Tablename) < 1)
                            {
                                this.SetError("No Results Found.");
                            }
                            else
                            {
                                this.SetError(busNSN.CHFInformationMessage);
                                this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
                                return true;
                            }
                        }
                        else
                        {
                            this.SetError(busNSN.CHARInformationMessage);
                            this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
                            return true;
                        }
                    }
                    else
                    {
                        this.SetError(busNSN.MDISInformationMessage);
                        this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
                        return true;
                    }
                }
                else
                {
                    this.SetError("No Results Found.");
                }
            }
            else
            {
                if (typeOfSearch == SearchTypes.PartNum)
                {
                    foreach (DataRow drResult in this.DataSet.Tables[this.Tablename].Rows)
                    {
                        //strNSNPartResults += String.Format("'{0}',",drResult["NSN"].ToString());
                        if (!strOriginalResultsOrder.Contains(String.Format("{0},", drResult["iPrimary"].ToString())))
                            strOriginalResultsOrder += String.Format("{0},", drResult["iPrimary"].ToString());
                        if (!strNSNPartResults.Contains(String.Format("(MCRL.fsc = '{0}' AND MCRL.NIIN = '{1}') OR", drResult["NSN"].ToString().Substring(0, 4), drResult["NSN"].ToString().Substring(4))))
                            strNSNPartResults += String.Format("(MCRL.fsc = '{0}' AND MCRL.NIIN = '{1}') OR", drResult["NSN"].ToString().Substring(0, 4), drResult["NSN"].ToString().Substring(4));
                    }

                    strOriginalResultsOrder = strOriginalResultsOrder.Substring(0, strOriginalResultsOrder.Length - 1);
                    strNSNPartResults = strNSNPartResults.Substring(0, strNSNPartResults.Length - 3);

                    Command = this.CreateCommand(string.Format(@"SELECT {0},
                                                                 CASE WHEN MCRL.iPrimary IN({4}) THEN 0 ELSE 1 END as lColor
                                                                    FROM {1}
                                                                    WHERE {2} 
                                                                    ORDER BY CASE WHEN MCRL.iPrimary IN({4}) THEN 0 ELSE 1 END
                                                                    LIMIT {3}",
                                                                    $"{this.SelectFieldsWithUserPreferences(this.Username)}, {codeFields}",
                                                                    $"{busNSN.StandardFromClause} {codeJoin}",
                                                                    strNSNPartResults,
                                                                    this.FetchSize.ToString(),
                                                                    strOriginalResultsOrder));

                    Command.Prepare();

                    //We know we would have results if we got this far so no need for another if/else.
                    this.ExecuteWithErrorLog(Command, this.Tablename);
                }


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
            return SelectFieldsWithUserPreferences(username, EnumEnhancedSearchResultsType.NONE);
        }
        protected string SelectFieldsWithUserPreferences(string username, EnumEnhancedSearchResultsType eEnhancedSearchResultsType)
        {
            busUserPreferences oUserPrefs = WebStoreFactory.GetBusUserPreferences();
            oUserPrefs.LoadByUserName(username);

            //Populate fetchsize property
            if (oUserPrefs.DataRow["FetchSize"] != DBNull.Value)
                this.FetchSize = Convert.ToInt32(oUserPrefs.DataRow["FetchSize"]);

            //Added specs (20,2) to the cast to prevent rounding
            string formatExpression = "CAST({0} AS Decimal(20,2))*" + Convert.ToString(oUserPrefs.DataRow["ConversionFactor"], new System.Globalization.CultureInfo("en-US"));
            string selectFields = eEnhancedSearchResultsType == EnumEnhancedSearchResultsType.NONE ? busNSN.StandardFieldList :
                eEnhancedSearchResultsType == EnumEnhancedSearchResultsType.MDIS ? busNSN.StandardMDISFieldList :
                eEnhancedSearchResultsType == EnumEnhancedSearchResultsType.CHF ? busNSN.StandardCHFFieldList : busNSN.StandardCHARFieldList;

            formatExpression = "ROUND(" + formatExpression + ",2)";

            if ((sbyte)oUserPrefs.DataRow["CurrencyJustification"] == 1)
                formatExpression = "CAST(CONCAT('" + oUserPrefs.DataRow["CurrencySymbol"].ToString().Replace("'", @"\'") + "'," + formatExpression + ") as char(30))";
            else
                formatExpression = "CAST(CONCAT(" + formatExpression + ",'" + oUserPrefs.DataRow["CurrencySymbol"].ToString().Replace("'", @"\'") + "') as char(30))";

            if (eEnhancedSearchResultsType != EnumEnhancedSearchResultsType.CHAR && eEnhancedSearchResultsType != EnumEnhancedSearchResultsType.CHF)
            {
                selectFields += "," + string.Format(formatExpression, "MDIS.price") + " as formatted_price";
            }
            else
            {
                selectFields += ", null as formatted_price";

                if (eEnhancedSearchResultsType == EnumEnhancedSearchResultsType.CHF)
                {
                    selectFields += "," + string.Format(formatExpression, "CHF.Unit_Price") + " as formatted_unit_price";
                    selectFields += "," + string.Format(formatExpression, "CHF.Total") + " as formatted_Total";
                }
            }

            return selectFields;
        }


        #endregion


        #endregion


    }
}