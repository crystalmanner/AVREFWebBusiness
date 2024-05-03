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
    public class busMCRL : busDataBase
    {
        public const string StandardFieldList = @"CONCAT(MCRL.fsc,MCRL.niin) as nsn,
                                                  MCRL.item_nam as description,
                                                  MCRL.ref_numb,
                                                  MCRL.ref_numb_no_dash,
                                                  MCRL.cage_cd_92 as cage,
                                                  MCRL.dup_isc,
                                                  MCRL.rncc,
                                                  MCRL.rnvc,
                                                  MCRL.hcc,
                                                  MCRL.msds,
                                                  MCRL.sadc,
                                                  MCRL.dup_da,
                                                  MCRL.hmic,
                                                  MCRL.inc_code";

        public const string StandardFromClause = @"MCRL";

        private string username = "";

        public string Username
        {
            get { return username; }
            set { username = value; }
        }

        #region Constructor
        public busMCRL()
            : base()
        {
            this.Tablename = "MCRL";
            this.PkField = "iPrimary";
            this.PkType = PkFieldTypes.intType;
        }
        #endregion

        #region DataSets
        public bool GetEmptyDataSet()
        {
            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}                                                      
                                                      FROM {1}
                                                      WHERE MCRL.iPrimary = -1", busMCRL.StandardFieldList, busMCRL.StandardFromClause));

            return this.Execute(Command, this.Tablename) == 0;

        }

        #endregion

        #region Functions
        /// <summary>
        /// Searches for and populates a dataset of all MCRL data that match the specified value.
        /// </summary>
        /// <param name="valueToFind">The ref/part number,nsn, or cage to search for.</param>
        /// <param name="typeOfSearch">The type of search to perform the search on. See <see cref="SearchTypes.cs">SearchTypes.cs</see> for a list of available types.</param>
        /// <param name="removeHyphens">Indicates if the search should be performed with hyphens removed.</param>
        /// <returns>True if one or more results are found</returns>
        public bool SearchMCRL(string valueToFind, SearchTypes typeOfSearch, bool exactMatch, bool removeHyphens)
        {
            string whereClause = String.Empty;
            string fieldExpression;
            string comparisonOperator;
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
                    fieldExpression = "CONCAT(MCRL.fsc, MCRL.niin)";
                    break;
                case SearchTypes.CAGE:
                    fieldExpression = "REPLACE(REPLACE(MCRL.Cage_cd_92, '-', ''), ' ', '')";
                    break;
                case SearchTypes.PartNum:
                default:
                    fieldExpression = "MCRL.ref_numb_no_dash";
                    break;
            }


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

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                                    FROM {1}
                                                                    WHERE {2}", busMCRL.StandardFieldList, busMCRL.StandardFromClause, whereClause));

            Command.Parameters.Add(this.CreateParameter("?valueToFind", valueToFind));
            Command.Prepare();

            if (this.Execute(Command, this.Tablename) < 1)
                this.SetError("No Results Found.");
            else
            {
                this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
                return true;
            }

            return false;

        }
        #endregion


    }
}