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
    public class busParts : busDataBase
    {
        public const string StandardFieldList = @"Parts.p_sysid,
                                                        Parts.p_part_name,
                                                        Parts.p_part_num,
                                                        Parts.p_part_num_no_dash,
                                                        Parts.p_group_num,
                                                        Parts.p_cage,
                                                        Parts.p_nsn,
                                                        Parts.p_model,
                                                        Parts.p_comment,
                                                        PartInfo.p_descript,
                                                        PartInfo.p_price,
                                                        PartInfo.p_condition,
                                                        PartInfo.p_type,
                                                        PartInfo.p_update,
                                                        PartInfo.p_disc,
                                                        PartInfo.p_pkgs,
                                                        PartInfo.p_deposit,
                                                        PartInfo.p_unit,
                                                        PartInfo.p_sscd,
                                                        PartInfo.p_source,
                                                        PartInfo.price2,
                                                        PartInfo.p_status,
                                                        Resource.r_name,
                                                        Resource.r_phone,
                                                        CAST(CAST(PartInfo.p_price as Decimal(20,2))*(1-COALESCE(" + busParts.MetaDatabase + ".Discounts.d_percent,0)/100) as Decimal(20,2)) as discountedprice";
        public const string StandardFromClause = @"Parts   
                                             INNER JOIN PartInfo On Parts.p_sysid=PartInfo.p_sysid AND PartInfo.tLastUpdt is not NULL
                                             INNER JOIN Resource On Parts.p_sysid=Resource.p_sysid AND Resource.tLastUpdt is not NULL
                                             LEFT JOIN " + busParts.MetaDatabase + ".Discounts ON " + busParts.MetaDatabase + ".Discounts.d_name=Resource.r_name AND " + busParts.MetaDatabase + ".Discounts.d_disc=PartInfo.p_disc";



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
        public busParts()
            : base()
        {
            this.Tablename = "Parts";
            this.PkField = "p_sysID";
            this.PkType = PkFieldTypes.intType;
        }

        /// <summary>
        /// Currently used by the Windows service
        /// </summary>
        /// <param name="strConnectionString"></param>
        public busParts(string strConnectionString)
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
            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}                                                      
                                                      FROM {1}
                                                      WHERE Parts.p_sysID = -1 AND {2}", this.SelectFieldsWithUserPreferences(this.Username), busParts.StandardFromClause + this.DiscountCompanyFilter(this.Username), this.PartSourceFilter(this.Username)));

            return this.ExecuteWithErrorLog(Command, this.Tablename) == 0;

        }

        public bool LoadByPartID(int nPartID, string strTableName)
        {
            bool retVal = false;

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM {1}                 
                                                      WHERE Parts.p_sysID=?PartID AND Parts.tLastUpdt is not NULL AND {2}", this.SelectFieldsWithUserPreferences(this.Username), busParts.StandardFromClause + this.DiscountCompanyFilter(this.Username), this.PartSourceFilter(this.Username)));
            Command.Parameters.Add(this.CreateParameter("?PartID", nPartID));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("Part not found.");
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

        public bool LoadAltPartNums(int nGroup, string strTableName)
        {
            bool retVal = false;

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                      FROM {1}                        
                                                      WHERE Parts.p_group_num=?GroupNum AND Parts.tLastUpdt is not NULL AND {2}", this.SelectFieldsWithUserPreferences(this.Username), busParts.StandardFromClause + this.DiscountCompanyFilter(this.Username), this.PartSourceFilter(this.Username)));
            Command.Parameters.Add(this.CreateParameter("?GroupNum", nGroup.ToString()));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("Part not found.");
                retVal = false;
            }
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                retVal = true;
            }

            return retVal;
        }

        public bool LoadAltParts(int nGroup, string strTableName)
        {
            bool retVal = false;

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                      FROM {1}                        
                                                      WHERE Parts.p_group_num=?GroupNum AND Parts.tLastUpdt is not NULL AND {2}", this.SelectFieldsWithUserPreferences(this.Username), busParts.StandardFromClause + this.DiscountCompanyFilter(this.Username), this.PartSourceFilter(this.Username)));
            Command.Parameters.Add(this.CreateParameter("?GroupNum", nGroup.ToString()));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("Part not found.");
                retVal = false;
            }
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                retVal = true;
            }

            return retVal;
        }
        public bool LoadAltParts(int nGroup)
        {
            return this.LoadAltParts(nGroup, this.DefaultViewName);
        }

        public bool LoadByCage(string strCage, string strTableName)
        {
            bool retVal = false;

            StringBuilder sb = new StringBuilder();
            foreach (char cCurrent in strCage)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb.Append(cCurrent);
                }
            }
            strCage = sb.ToString();

            string search = $"LIKE CONCAT('{strCage}', '%')";

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                      FROM {1}                        
                                                      WHERE REPLACE(REPLACE(Parts.p_cage, '-', ''), ' ', '') ?Cage AND Parts.tLastUpdt is not NULL AND {2} LIMIT {3}", this.SelectFieldsWithUserPreferences(this.Username), busParts.StandardFromClause + this.DiscountCompanyFilter(this.Username), this.PartSourceFilter(this.Username), this.FetchSize.ToString()));
            Command.Parameters.Add(this.CreateParameter("?Cage", strCage));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("No Parts Found.");
                retVal = false;
            }
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                retVal = true;
            }

            return retVal;
        }
        public bool LoadByCage(string strCage)
        {
            return this.LoadByCage(strCage, this.DefaultViewName);
        }

        /// <summary>
        /// Retreives all active parts from the database.
        /// </summary>
        /// <remarks>It is highly recommended that this function not be used. The data is more than 3 Gb in size and 15+ million records. Data should be filtered in some way.</remarks>
        /// <returns>True if one or more rows are returned</returns>
        public bool LoadAllParts()
        {
            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                      FROM {1}
                                                        WHERE Parts.tLastUpdt is not NULL AND {2}", this.SelectFieldsWithUserPreferences(this.Username), busParts.StandardFromClause + this.DiscountCompanyFilter(this.Username), this.PartSourceFilter(this.Username)));

            if (this.ExecuteWithErrorLog(Command, this.Tablename) < 1)
                this.SetError("Part not found.");
            else
            {
                this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
                return true;
            }

            return false;
        }

        #endregion

        #region Searches
        /// <summary>
        /// Searches for and populates a dataset of all parts that match the specified value.
        /// </summary>
        /// <param name="valueToFind">The part number to search for.</param>
        /// <param name="typeOfSearch">The type of part number to perform the search on. See <see cref="SearchTypes.cs">SearchTypes.cs</see> for a list of available types.</param>
        /// <param name="removeHyphens">Indicates if the search should be performed with hyphens removed.</param>
        /// <returns>True if one or more parts are found</returns>
        public bool SearchParts(string valueToFind, SearchTypes typeOfSearch, bool exactMatch = false, bool removeHyphens = true)
        {
            string whereClause = "Parts.tLastUpdt is not NULL";
            //string strAltPartCountClause = String.Empty;
            string fieldExpression;
            string comparisonOperator;
            string searchType;

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
                    valueToFind = valueToFind.Replace("-", String.Empty);
                    fieldExpression = "REPLACE(REPLACE(Parts.p_nsn, '-', ''), ' ', '')";
                    searchType = "Part";
                    break;
                case SearchTypes.Manufacturer:
                    fieldExpression = "REPLACE(REPLACE(Resource.r_name, '-', ''), ' ', '')";
                    searchType = "Manufacturer";
                    break;
                case SearchTypes.CAGE:
                    fieldExpression = "REPLACE(REPLACE(Parts.p_cage, '-', ''), ' ', '')";
                    searchType = "Cage";
                    break;
                case SearchTypes.Description:
                    fieldExpression = "REPLACE(REPLACE(PartInfo.p_descript, '-', ''), ' ', '')";
                    searchType = "Description";
                    break;

                case SearchTypes.PartNum:
                default:
                    //08-09-2009: Always removing hyphens unless exact match is checked (still leaving as a parameter incase mind is changed)
                    if (!exactMatch || removeHyphens)
                    {
                        fieldExpression = "Parts.p_part_num_no_dash";

                        // foreach (char cCurrent in valueToFind)
                        // {
                        //     if (!char.IsLetterOrDigit(cCurrent))
                        //         valueToFind = valueToFind.Replace(cCurrent.ToString(), String.Empty).Trim();
                        // }

                        //valueToFind = valueToFind.Replace("-", "");
                    }
                    else
                        fieldExpression = "Parts.p_part_num_no_dash";

                    searchType = "Part";
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

            if (typeOfSearch == SearchTypes.Description && !exactMatch)
                whereClause += $" AND MATCH({fieldExpression}) AGAINST (?valueToFind in boolean mode)";
            else
                whereClause += $" AND {fieldExpression} {comparisonOperator} ?valueToFind";

            var exCodesJoin = @"LEFT JOIN ExportCodes ON Parts.p_SysID = ExportCodes.P_SysID
                                LEFT JOIN NsnExportCodes ON NsnExportCodes.P_NSN = Parts.P_NSN
                                LEFT JOIN altparts ON Parts.p_sysID = altparts.p_sysid";

            //strAltPartCountClause = @"LEFT JOIN (SELECT Parts.p_group_num, COUNT(Parts.p_group_num) as nAltPartCount FROM Parts INNER JOIN PartInfo On Parts.p_sysid=PartInfo.p_sysid AND PartInfo.tLastUpdt is not NULL INNER JOIN Resource On Parts.p_sysid=Resource.p_sysid AND Resource.tLastUpdt is not NULL WHERE " + whereClause + " GROUP BY Parts.p_group_num) P1 ON P1.p_group_num = Parts.p_group_num";
            var selectFields = $"{this.SelectFieldsWithUserPreferences(this.Username)}, CAST(IFNULL((SELECT COUNT(P1.p_Group_num) FROM Parts P1 WHERE P1.p_group_num = Parts.p_group_num)-1,'0') as signed) as nAltPartCount, ExportCodes.HtsCode, COALESCE(NSNExportCodes.SchbCode, ExportCodes.SchbCode) as SchbCode, ExportCodes.EccnCode, altparts.a_parts";
            var fromClause = $"{busParts.StandardFromClause} {this.DiscountCompanyFilter(this.Username)} {exCodesJoin}";
            //IDbCommand Command = this.CreateCommand($@"SELECT {selectFields},
            //                                                CAST(CASE p_source
            //                                                    WHEN 'AVREF' THEN 1
            //                                                    WHEN 'AVIALL' THEN 2
            //                                                    ELSE 9
            //                                                    END as unsigned) as Sort1,
            //                                                REPLACE(p_part_num, '-', '') as sort2,
            //                                                CAST(CASE p_source
            //                                                    WHEN 'AVREF' THEN p_update
            //                                                    ELSE null
            //                                                    END as date) as Sort3
            //                                            FROM {fromClause}
            //                                            WHERE ({whereClause}) 
            //                                            AND {this.PartSourceFilter(this.Username)}
            //                                            ORDER BY sort1, sort2, sort3 desc
            //                                            LIMIT {this.FetchSize}");

            IDbCommand Command = this.CreateCommand($@"SELECT {selectFields}
                                                      FROM {fromClause}
                                                      WHERE ({whereClause}) AND {this.PartSourceFilter(this.Username)}
                                                      LIMIT {this.FetchSize}");

            //IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}
            //                                          FROM {1}
            //                                          WHERE ({2}) AND {3} LIMIT {4}",
            //                                         this.SelectFieldsWithUserPreferences(this.Username) + ", CAST(IFNULL((SELECT COUNT(P1.p_Group_num) FROM Parts P1 WHERE P1.p_group_num = Parts.p_group_num)-1,'0') as signed) as nAltPartCount, ExportCodes.HtsCode, COALESCE(NSNExportCodes.SchbCode, ExportCodes.SchbCode) as SchbCode, ExportCodes.EccnCode",
            //                                         $"{busParts.StandardFromClause} {this.DiscountCompanyFilter(this.Username)} {exCodesJoin}",
            //                                         whereClause,
            //                                         this.PartSourceFilter(this.Username),
            //                                         this.FetchSize.ToString()));

            Command.Parameters.Add(this.CreateParameter("?valueToFind", valueToFind));
            Command.Prepare();

            if (typeOfSearch == SearchTypes.Description)
                Command.CommandText = "SET SQL_BIG_SELECTS=1;" + Command.CommandText; // Otherwise we get the error: "The SELECT would examine more than MAX_JOIN_SIZE rows; check your WHERE and use SET SQL_BIG_SELECTS=1 or SET SQL_MAX_JOIN_SIZE=# if the SELECT is okay ". Also must be part of the same command.


            if (this.ExecuteWithErrorLog(Command, this.Tablename) < 1)
                this.SetError(searchType + " not found.");
            else
            {
                this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
                return true;
            }

            return false;
        }

        public bool SearchPMAParts(string valueToFind, SearchTypes typeOfSearch)
        {
            if (typeOfSearch == SearchTypes.PartNum || typeOfSearch == SearchTypes.PMA)
            {
                IDbCommand PMACommand = this.CreateCommand(GetPmaSqlString(valueToFind));

                return this.ExecuteWithErrorLog(PMACommand, "PMAInfo") > 0;
            }

            return false;
        }

        public bool SearchMFGPriceHistoryParts(string valueToFind, SearchTypes typeOfSearch)
        {
            bool exactMatch = true;
            //passes in exactMatch true when doing partnum search
            if (typeOfSearch == SearchTypes.PartNum)
            {
                IDbCommand MFGPriceHistoryCommand = this.CreateCommand(GetMFGPriceHistorySqlString(valueToFind, false, exactMatch));
                return this.ExecuteWithErrorLog(MFGPriceHistoryCommand, "MFGPriceHistoryInfo") > 0;
            }

            //passes in exactMatch false when doing mfgPriceHistory Search
            else if (typeOfSearch == SearchTypes.MFGPriceHistory)
            {
                IDbCommand MFGPriceHistoryCommand = this.CreateCommand(GetMFGPriceHistorySqlString(valueToFind, false, !exactMatch));
                return this.ExecuteWithErrorLog(MFGPriceHistoryCommand, "MFGPriceHistoryInfo") > 0;
            }

            return false;
        }

        #region Batch Searches
        /// <summary>
        /// Searches for and populates a dataset of all parts that match the specified criteria.
        /// </summary>
        /// <param name="dtBatchSearchCriteria">A datatable containing the criteria for the search. Column names in the table are p_part_num, exactmatch, and p_cage</param>
        /// <returns>True if one or more parts are found</returns>
        public bool BatchSearchPartsByPartNum(DataTable dtBatchSearchCriteria)
        {
            return this.BatchSearchParts(dtBatchSearchCriteria, "p_part_num", "p_part_num");
        }


        /// <summary>
        /// Searches for and populates a dataset of all parts that match the specified criteria.
        /// </summary>
        /// <param name="dtBatchSearchCriteria">A datatable containing the criteria for the search. Column names in the table are p_nsn, exactmatch, and p_cage</param>
        /// <returns>True if one or more parts are found</returns>
        public bool BatchSearchPartsByNSN(DataTable dtBatchSearchCriteria)
        {
            return this.BatchSearchParts(dtBatchSearchCriteria, "p_nsn", "p_nsn");
        }



        /// <summary>
        /// Searches for and populates a dataset of all parts that match the specified criteria.
        /// </summary>
        /// <param name="dtBatchSearchCriteria">A datatable containing the criteria for the search. Column names in the table are p_part_num, exactmatch, and p_cage</param>
        /// <param name="criteriaMainFieldName">The name of the main field to search on in dtBatchSearchCriteria</param>
        /// <param name="tableMainFieldName">The name of the main field to search on in the Parts table</param>
        /// <returns>True if one or more parts are found</returns>
        public bool BatchSearchParts(DataTable dtBatchSearchCriteria, string criteriaMainFieldName, string tableMainFieldName)
        {
            if (dtBatchSearchCriteria.Rows.Count == 0)
                this.SetError("No Criteria Specified.");
            else
            {

                string whereClause = "Parts.tLastUpdt is not NULL";
                string[] conditions = new string[dtBatchSearchCriteria.Rows.Count]; // For using string.Join to add the boolean operators between them
                ArrayList selectParameters = new ArrayList();
                int conditionCount = 0;

                string valueToFind, cageCode;
                bool exactMatch;
                string currentCondition;
                string comparisonOperator;
                IDbDataParameter currentParameter;

                foreach (DataRow dr in dtBatchSearchCriteria.Rows)
                {
                    valueToFind = dr[criteriaMainFieldName].ToString().Trim();

                    StringBuilder sb = new StringBuilder();
                    foreach (char cCurrent in valueToFind)
                    {
                        if (char.IsLetterOrDigit(cCurrent))
                        {
                            sb.Append(cCurrent);
                        }
                    }
                    valueToFind = sb.ToString();

                    cageCode = dr["p_cage"].ToString().Trim();
                    // Normalize cageCode by removing special and non-alphanumeric characters.
                    StringBuilder sb2 = new StringBuilder();
                    foreach (char cCurrent in cageCode)
                    {
                        if (char.IsLetterOrDigit(cCurrent))
                        {
                            sb2.Append(cCurrent);
                        }
                    }
                    cageCode = sb2.ToString();

                    // exactMatch = (bool)dr["exactmatch"];
                    exactMatch = false;
                    if (exactMatch)
                    {
                        comparisonOperator = "=";
                    }
                    else
                    {
                        comparisonOperator = "LIKE";
                        valueToFind += "%";
                    }

                    currentParameter = this.CreateParameter("?ValueToFind" + conditionCount.ToString(), valueToFind);
                    currentCondition = string.Format("REPLACE(REPLACE(Parts.{0}, '-', ''), ' ', '') {1} {2}", tableMainFieldName, comparisonOperator, currentParameter.ParameterName);
                    selectParameters.Add(currentParameter);

                    if (!cageCode.Equals(string.Empty))
                    {
                        cageCode += "%";
                        currentParameter = this.CreateParameter("?CageCode" + conditionCount.ToString(), cageCode);
                        currentCondition += string.Format(" AND Parts.p_cage LIKE {0}", currentParameter.ParameterName);
                        selectParameters.Add(currentParameter);
                    }

                    conditions[conditionCount] = string.Format("({0})", currentCondition);

                    conditionCount++;
                }
                whereClause += string.Format(" AND ({0})", string.Join(" OR ", conditions));

                IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                      FROM {1}
                                                        WHERE ({2}) AND {3} LIMIT {4}", this.SelectFieldsWithUserPreferences(this.Username) + ", CAST(IFNULL((SELECT COUNT(P1.p_Group_num) FROM Parts P1 WHERE P1.p_group_num = Parts.p_group_num)-1,'0') as signed) as nAltPartCount", busParts.StandardFromClause + this.DiscountCompanyFilter(this.Username), whereClause, this.PartSourceFilter(this.Username), this.FetchSize.ToString()));

                foreach (object param in selectParameters)
                {
                    Command.Parameters.Add((IDbDataParameter)param);
                }
                Command.Prepare();

                if (this.ExecuteWithErrorLog(Command, this.Tablename) < 1)
                    this.SetError("Part not found.");
                else
                {
                    this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
                    return true;
                }

            }

            return false;

        }
        #endregion

        #endregion

        #endregion

        #region Functions

        #region Bulk Import
        /// <summary>
        /// The 3 Data Files Should Be In CSV Format
        /// </summary>
        /// <param name="strFileName"></param>
        /// <returns></returns>
        public bool ImplementNewDataFile(string strPartInfoFile, string strPartsFile, string strResourceFile)
        {
            busParts oParts = WebStoreFactory.GetBusParts();
            DataTable dtParts = new DataTable();
            DataTable dtPartInfo = new DataTable();
            DataTable dtResource = new DataTable();
            OleDbDataReader oPartsReader;
            OleDbDataReader oPartInfoReader;
            OleDbDataReader oResourceReader;
            OleDbConnection foxCon1 = new OleDbConnection();
            OleDbCommand foxCommand1 = new OleDbCommand();
            OleDbConnection foxCon2 = new OleDbConnection();
            OleDbCommand foxCommand2 = new OleDbCommand();
            OleDbConnection foxCon3 = new OleDbConnection();
            OleDbCommand foxCommand3 = new OleDbCommand();
            //OleDbDataAdapter daFox = new OleDbDataAdapter();
            System.Data.SqlClient.SqlBulkCopy sqlBulk = new System.Data.SqlClient.SqlBulkCopy(this.ConnectionString);
            DateTime dtValue;
            //int nRecords = 0;
            int nCount = 0;

            this.GetEmptyDataSet();

            sqlBulk.BulkCopyTimeout = Int32.MaxValue;
            sqlBulk.DestinationTableName = this.Tablename;

            //Retrieve Parts
            foxCon1.ConnectionString = "Provider=vfpoledb.1;Data Source=" + strPartsFile + ";Collating Sequence=general";
            foxCon1.Open();
            foxCommand1.Connection = foxCon1;
            foxCommand1.CommandText = "SELECT * FROM " + strPartsFile;
            oPartsReader = foxCommand1.ExecuteReader();
            //daFox = new OleDbDataAdapter(foxCommand);
            //nRecords = daFox.Fill(dtParts);
            //foxCon.Close();

            //Retrieve PartInfo
            foxCon2.ConnectionString = "Provider=vfpoledb.1;Data Source=" + strPartInfoFile + ";Collating Sequence=general";
            foxCon2.Open();
            foxCommand2.Connection = foxCon2;
            foxCommand2.CommandText = "SELECT * FROM " + strPartInfoFile;
            oPartInfoReader = foxCommand2.ExecuteReader();
            //daFox = new OleDbDataAdapter(foxCommand);
            //daFox.Fill(dtPartInfo);
            //foxCon.Close();

            //Retrieve Resource
            foxCon3.ConnectionString = "Provider=vfpoledb.1;Data Source=" + strResourceFile + ";Collating Sequence=general";
            foxCon3.Open();
            foxCommand3.Connection = foxCon3;
            foxCommand3.CommandText = "SELECT * FROM " + strResourceFile;
            oResourceReader = foxCommand3.ExecuteReader();
            //daFox = new OleDbDataAdapter(foxCommand);
            //daFox.Fill(dtResource);
            //foxCon.Close();

            //for (int nIndex = 0; nIndex < nRecords; nIndex++)
            //{
            while (oPartsReader.Read())
            {
                nCount++;
                oPartInfoReader.Read();
                oResourceReader.Read();

                DataRow drNewRow = this.DataSet.Tables[this.Tablename].NewRow();
                //Data From Parts File
                drNewRow["p_sysid"] = Convert.ToInt32(oPartsReader["p_sysid"]);
                drNewRow["p_part_name"] = oPartsReader["p_part_nam"].ToString().Trim();
                drNewRow["p_part_num"] = oPartsReader["p_part_num"].ToString().Trim();
                drNewRow["p_group_num"] = Convert.ToInt32(oPartsReader["p_group_nu"]);
                drNewRow["p_cage"] = oPartsReader["p_cage"].ToString().Trim();
                drNewRow["p_nsn"] = oPartsReader["p_nsn"].ToString().Trim();
                drNewRow["p_model"] = oPartsReader["p_model"].ToString().Trim();
                drNewRow["p_comment"] = oPartsReader["p_comment"].ToString().Trim();

                //Data From Part Info File
                drNewRow["p_descript"] = oPartInfoReader["p_descript"].ToString().Trim();
                drNewRow["p_price"] = oPartInfoReader["p_price"].ToString().Trim();
                drNewRow["p_condition"] = oPartInfoReader["p_condit"].ToString().Trim();
                drNewRow["p_type"] = oPartInfoReader["p_type"].ToString().Trim();
                drNewRow["p_update"] = DateTime.TryParse(oPartInfoReader["p_update"].ToString().Trim(), out dtValue) ? dtValue : DateTime.Today;
                drNewRow["p_disc"] = oPartInfoReader["p_disc"].ToString().Trim();
                drNewRow["p_pkgs"] = oPartInfoReader["p_pkgs"].ToString().Trim();
                drNewRow["p_deposit"] = oPartInfoReader["p_deposit"].ToString().Trim();
                drNewRow["p_unit"] = oPartInfoReader["p_unit"].ToString().Trim();
                drNewRow["p_sscd"] = oPartInfoReader["p_sscd"].ToString().Trim();
                drNewRow["p_source"] = oPartInfoReader["p_source"].ToString().Trim();
                drNewRow["price2"] = oPartInfoReader["price2"].ToString().Trim();
                drNewRow["p_status"] = oPartInfoReader["p_status"].ToString().Trim();

                //Data From Resource File
                drNewRow["r_name"] = oResourceReader["r_name"].ToString().Trim();
                drNewRow["r_phone"] = oResourceReader["r_phone"].ToString().Trim();

                drNewRow["lActive"] = false;
                this.DataSet.Tables[this.Tablename].Rows.Add(drNewRow);

                if (nCount == 200000)
                {
                    nCount = 0;
                    sqlBulk.WriteToServer(this.DataSet.Tables[this.Tablename]);
                    this.DataSet.Tables[this.Tablename].Clear();
                }
            }

            sqlBulk.WriteToServer(this.DataSet.Tables[this.Tablename]);
            oPartInfoReader.Close();
            oPartsReader.Close();
            oResourceReader.Close();

            return !this.Error;
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
            string selectFields = busParts.StandardFieldList;

            formatExpression = "ROUND(" + formatExpression + ",2)";

            if ((sbyte)oUserPrefs.DataRow["CurrencyJustification"] == 1)
                formatExpression = "CAST(CONCAT('" + oUserPrefs.DataRow["CurrencySymbol"].ToString().Replace("'", @"\'") + "'," + formatExpression + ") as char(30))";
            else
                formatExpression = "CAST(CONCAT(" + formatExpression + ",'" + oUserPrefs.DataRow["CurrencySymbol"].ToString().Replace("'", @"\'") + "') as char(30))";

            selectFields += "," + string.Format(formatExpression, "PartInfo.p_price") + " as formatted_p_price";
            selectFields += "," + string.Format(formatExpression, "PartInfo.p_deposit") + " as formatted_p_deposit";
            selectFields += "," + string.Format(formatExpression, "PartInfo.price2") + " as formatted_price2";
            selectFields += "," + string.Format(formatExpression, "CAST(CAST(PartInfo.p_price as Decimal(20,2)) * (1 - COALESCE(" + busParts.MetaDatabase + ".Discounts.d_percent, 0) / 100) as Decimal(20,2))") + " as formatted_discounted_price";


            return selectFields;
        }

        public bool GetPmaInfo(string partId)
        {
            return this.ExecuteWithErrorLog(GetPmaSqlString(partId, true), "PMAIdInfo") > 0;
        }

        public string GetPmaSqlString(string searchVal,  bool searchById = false)
        {
            //string search = exactMatch ? $"=\"{searchVal}\"" : $"LIKE \"%{searchVal}%\"";
            StringBuilder sb = new StringBuilder();
            foreach (char cCurrent in searchVal)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb.Append(cCurrent);
                }
            }
            searchVal = sb.ToString();
            
            string search = $"LIKE \"%{searchVal}%\"";
            string where = $"PartNumber {search} OR ReplacedPartNumber {search}" ;

            //this overwrites either of the other two, called when running a report
            if (searchById)
            {
                where = $"PartId =\"{searchVal}\"";
            }

            var retVal = $@"SELECT Holder, PMANumber, Address, City, State, ZIP, Country, ResponsibleOfficeID as Office, SupNumber, 
                            SupDate, PartNumber, PartName, ReplacedPartNumber, ApprovalBasis, Models , PartId
                                                        FROM pmaparts
                                                        WHERE {where}
                                                        LIMIT {this.FetchSize}";
            return retVal;
        }
        #endregion


        public bool GetMFGPriceHistoryInfo(string partId)
        {
            return this.ExecuteWithErrorLog(GetMFGPriceHistorySqlString(partId), "MFGPriceHistoryIdInfo") > 0;
        }

        public string GetMFGPriceHistorySqlString(string searchVal, bool searchById = false, bool exactMatch = false)
        {
            String where;
            StringBuilder sb = new StringBuilder();
            foreach (char cCurrent in searchVal)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb.Append(cCurrent);
                }
            }
            searchVal = sb.ToString();

            if (exactMatch)
            {
                where = $"p_part_num =\"{searchVal}\"";
            }
            else
            {
                // string searchValNoDash = searchVal.Replace("-", String.Empty);
                // where = $"p_part_num_no_dash LIKE \"%{searchValNoDash}%\"";
                // string searchValNoDash = searchVal.Replace("-", String.Empty);
                StringBuilder sb2 = new StringBuilder();
                foreach (char cCurrent in searchVal)
                {
                    if (char.IsLetterOrDigit(cCurrent))
                    {
                        sb2.Append(cCurrent);
                    }
                }
                string searchValNoDash = sb2.ToString();
                where = $"REPLACE(REPLACE(p_part_num, '-', ''), ' ', '') LIKE \"%{searchValNoDash}%\"";
            }

            //this overwrites either of the other two, called when running a report
            if (searchById)
            {
                where = $"PartId =\"{searchVal}\"";
            }

            var retVal = $@"SELECT p_part_num, p_type, p_price, p_disc, p_deposit, p_condition, p_unit, if(p_update > '01000101', p_update, null) as p_update, p_pkgs, p_descript
                                                        FROM pricehistory
                                                        WHERE {where}
                                                        ORDER BY p_type ASC, p_descript ASC, p_update DESC
                                                        LIMIT {this.FetchSize}";
            return retVal;
        }

        #region Part Source Filter
        /// <summary>
        /// Adds the conditions needed for the where clause to retrieve only the data the customer has access to.
        /// </summary>
        /// <param name="username">Username to get the access for.</param>
        /// <returns>The where clause condition to be used.</returns>
        protected string PartSourceFilter(string username)
        {
            busCompanies oCompanies = WebStoreFactory.GetBusCompanies();
            string filterCondition = "";

            if (oCompanies.LoadByUsername(username))
            {
                if (!(bool)oCompanies.DataRow["UsesAVREF"])
                    filterCondition += "PartInfo.p_source!=\"AVREF\" AND ";
                if (!(bool)oCompanies.DataRow["UsesMCRL"])
                    filterCondition += "PartInfo.p_source!=\"MCRL\" AND ";

                if (filterCondition.Contains(" AND"))
                    filterCondition = filterCondition.Remove(filterCondition.LastIndexOf(" AND"));

                if (filterCondition.Equals(string.Empty))
                    filterCondition = "true";
            }
            else
                filterCondition = "false";


            return "(" + filterCondition + ")"; // Place holder until actual code is added
        }

        #endregion

        #region DiscountCompanyFilter
        protected string DiscountCompanyFilter(string strUsername)
        {
            busCompanies oCompanies = WebStoreFactory.GetBusCompanies();


            return oCompanies.LoadByUsername(strUsername) ? String.Format(" AND Discounts.CompanyID = {0}", oCompanies.DataRow["CompanyID"].ToString()) : String.Empty;
        }
        #endregion

        #endregion


    }
}
