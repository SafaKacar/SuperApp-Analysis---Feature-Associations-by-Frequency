USE [DWH_DB]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[spMainFeatureRelations]
as

DECLARE 
		@MainFeatureCombLastDate as INT = (select MAX(MonthKey) from [DWH_DB].[dbo].[FACT_MainFeatureCombinations] with (nolock))
drop table if exists ##FeatureInteractionBehaviorsReferenceTemp
DELETE FROM [DWH_DB].[dbo].[FACT_MainFeatureRelations] WHERE MonthKey = @MainFeatureCombLastDate

SELECT	A.MonthKey
		 ,FeatureTypeCount
		/*MERGERS*/			
			,A.FeatureTypeComb
			/*MERGERS (FeatureType Grouped)*/
			/*MERGERS (TRANSACTIONS [Tx.#/Tx.V./ABS(Tx.V.)*/
			,FeatureTypeCombUptoTxCount
			,FeatureTypeCombUptoTxVolume				,		FeatureTypeCombUptoAbsTxVolume	
			/*MERGERS (Averages)*/
			/*,AvgResultingBalancebyFeatureType*/--		,		AvgTxVolumePerCapitaForAgg
--			,AvgTxCountPerCapitaForAgg				,		AvgTicketSizeForAgg
			/*MERGERS (Rates)*/
--			,TxCountRates,AbsTxVolumeRates
		/*DIVISIONS*/
			/*DIVISIONS (FeatureType Divided)*/
			,A.FeatureTypeCombDivided
			/*DIVISIONS (TRANSACTIONS [Tx.#/Tx.V./ABS(Tx.V.)*/
			,A1.FeatureTypeCombUptoTxCountDivided
			,A2.FeatureTypeCombUptoTxVolumeDivided	 ,		A3.FeatureTypeCombUptoAbsTxVolumeDivided		
			/*DIVISIONS (Averages)*/
			/*,A4.AvgResultingBalancebyFeatureTypeDivided*/	/*A5.AvgTxVolumePerCapitaForAggDivided
			,A6.AvgTxCountPerCapitaForAggDivided	 		A7.AvgTicketSizeForAggDivided
			DIVISIONS (Rates)
			,A8.TxCountRatesDivided					 ,		A9.AbsTxVolumeRatesDivided*/
		/*OVERALL ATTRIBUTE CALCULATIONS*/
			/*OVERALL CALCULATIONS (TRANSACTIONS [UU/Tx.#/Tx.V./ABS(Tx.V.)*/
			,UserCount
			--,AvgAge
--			,SumTxCount, SumAbsTxVolume ,SumTxVolume
			/*OVERALL CALCULATIONS (AVERAGES)*/
			--,AvgTicketSize							,		AvgResultingBalance
			--,AvgTxCountPerCapita					,		AvgTxVolumePerCapita
		/*RANKINGS BY OVERALL CALCULATIONS*/
			--,RankByUserCount						,		RankBySumTxCount
			--,RankBySumAbsTxVolume					,		RankingMetricWeightedAverages
		/*PERCENTAGES BY OVERALL*/
			--,UserCountOverallPercentage				,		SumTxCountOverallPercentage
			--,SumAbsTxVolumeOverallPercentage		,		PercentageMetricWeightedAverages
INTO ##FeatureInteractionBehaviorsReferenceTemp
FROM
	(
			 (SELECT *						, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb order by (SELECT NULL)) irRN, CAST(VALUE AS decimal(5, 2)) FeatureTypeCombDivided				   FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(FeatureTypeComb				 ,'/')WHERE MonthKey = @MainFeatureCombLastDate) A
		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb order by (SELECT NULL)) i1RN, CAST(VALUE AS decimal(15,2)) FeatureTypeCombUptoTxCountDivided	   FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(FeatureTypeCombUptoTxCount	 ,'/')WHERE MonthKey = @MainFeatureCombLastDate) A1 ON A1.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A1.i1RN AND A.MonthKey = A1.MonthKey
		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb order by (SELECT NULL)) i2RN, CAST(VALUE AS decimal(15,2)) FeatureTypeCombUptoTxVolumeDivided	   FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(FeatureTypeCombUptoTxVolume	 ,'/')WHERE MonthKey = @MainFeatureCombLastDate) A2 ON A2.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A2.i2RN AND A.MonthKey = A2.MonthKey
		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb order by (SELECT NULL)) i3RN, CAST(VALUE AS decimal(15,2)) FeatureTypeCombUptoAbsTxVolumeDivided FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(FeatureTypeCombUptoAbsTxVolume ,'/')WHERE MonthKey = @MainFeatureCombLastDate) A3 ON A3.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A3.i3RN AND A.MonthKey = A3.MonthKey
--		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i4RN, CAST(VALUE AS decimal(15,2)) AvgResultingBalancebyFeatureTypeDivided FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(AvgResultingBalancebyFeatureType ,'/')WHERE MonthKey = @MainFeatureCombLastDate) A4 ON A4.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A4.i4RN AND A.MonthKey = A4.MonthKey
--		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i5RN, CAST(VALUE AS decimal(15,2)) AvgTxVolumePerCapitaForAggDivided	  FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(AvgTxVolumePerCapitaForAgg	  ,'/')WHERE MonthKey = @MainFeatureCombLastDate) A5 ON A5.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A5.i5RN AND A.MonthKey = A5.MonthKey
--		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i6RN, CAST(VALUE AS decimal(15,2)) AvgTxCountPerCapitaForAggDivided	  FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(AvgTxCountPerCapitaForAgg	  ,'/')WHERE MonthKey = @MainFeatureCombLastDate) A6 ON A6.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A6.i6RN AND A.MonthKey = A6.MonthKey
--		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i7RN, CAST(VALUE AS decimal(15,2)) AvgTicketSizeForAggDivided			  FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(AvgTicketSizeForAgg			  ,'/')WHERE MonthKey = @MainFeatureCombLastDate) A7 ON A7.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A7.i7RN AND A.MonthKey = A7.MonthKey
--		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i8RN, CAST(VALUE AS decimal(15,2)) TxCountRatesDivided					  FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(TxCountRates					  ,'/')WHERE MonthKey = @MainFeatureCombLastDate) A8 ON A8.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A8.i8RN AND A.MonthKey = A8.MonthKey
--		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i9RN, CAST(VALUE AS decimal(15,2)) AbsTxVolumeRatesDivided				  FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(AbsTxVolumeRates				  ,'/')WHERE MonthKey = @MainFeatureCombLastDate) A9 ON A9.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A9.i9RN AND A.MonthKey = A9.MonthKey
	)
;
WITH X  AS
	(
		SELECT 
			  DISTINCT
			  @MainFeatureCombLastDate MonthKey
			 ,FeatureTypeComb
			 ,FeatureTypeCombDivided
			 ,UserCount
			 --,AvgAge
			 ,FeatureTypeCombUptoTxCountDivided
			 ,FeatureTypeCombUptoTxVolumeDivided
			 ,FeatureTypeCombUptoAbsTxVolumeDivided
--			 ,AvgResultingBalancebyFeatureTypeDivided
--			 ,AvgTxVolumePerCapitaForAggDivided
--			 ,AvgTxCountPerCapitaForAggDivided
--			 ,AvgTicketSizeForAggDivided
--			 ,TxCountRatesDivided
--			 ,AbsTxVolumeRatesDivided
			 ,COUNT(*)								   OVER (PARTITION BY FeatureTypeCombDivided) CountFeatureTypeCombDivided
			 ,SUM(FeatureTypeCombUptoTxCountDivided)	   OVER (PARTITION BY FeatureTypeCombDivided) SumFeatureTypeCombUptoTxCountDivided
			 ,SUM(FeatureTypeCombUptoAbsTxVolumeDivided) OVER (PARTITION BY FeatureTypeCombDivided) SumFeatureTypeCombUptoAbsTxVolumeDivided
			,FeatureTypeCount
		/*Tx Count*/


			,CASE WHEN MAX(FeatureTypeCombUptoTxCountDivided) OVER (PARTITION BY FeatureTypeComb ORDER BY FeatureTypeCombUptoTxCountDivided DESC) 
						>= LEAD(FeatureTypeCombUptoTxCountDivided,1,FeatureTypeCombUptoTxCountDivided) OVER (PARTITION BY FeatureTypeComb  ORDER BY FeatureTypeCombUptoTxCountDivided DESC)
				   THEN LEAD(FeatureTypeCombDivided,2) OVER (PARTITION BY FeatureTypeComb  ORDER BY FeatureTypeCombUptoTxCountDivided DESC) ELSE NULL END Lag2ByTxCount

			,CASE WHEN MAX(FeatureTypeCombUptoTxCountDivided) OVER (PARTITION BY FeatureTypeComb ORDER BY FeatureTypeCombUptoTxCountDivided DESC) 
						>= LEAD(FeatureTypeCombUptoTxCountDivided,1,FeatureTypeCombUptoTxCountDivided) OVER (PARTITION BY FeatureTypeComb  ORDER BY FeatureTypeCombUptoTxCountDivided DESC)
				   THEN LEAD(FeatureTypeCombDivided,1) OVER (PARTITION BY FeatureTypeComb  ORDER BY FeatureTypeCombUptoTxCountDivided DESC) ELSE NULL END Lag1ClosestLowerByTxCount

		/*Tx. Volume*/


			,CASE WHEN MAX(ABS(FeatureTypeCombUptoTxVolumeDivided)) OVER (PARTITION BY FeatureTypeComb ORDER BY  ABS(FeatureTypeCombUptoTxVolumeDivided) DESC) 
						>= LEAD( ABS(FeatureTypeCombUptoTxVolumeDivided),1, ABS(FeatureTypeCombUptoTxVolumeDivided)) OVER (PARTITION BY FeatureTypeComb  ORDER BY  ABS(FeatureTypeCombUptoTxVolumeDivided) DESC)
				   THEN LEAD(FeatureTypeCombDivided,2) OVER (PARTITION BY FeatureTypeComb  ORDER BY  ABS(FeatureTypeCombUptoTxVolumeDivided) DESC) ELSE NULL END Lag2ByTxVolume


			,CASE WHEN MAX(ABS(FeatureTypeCombUptoTxVolumeDivided)) OVER (PARTITION BY FeatureTypeComb ORDER BY  ABS(FeatureTypeCombUptoTxVolumeDivided) DESC) 
						>= LEAD( ABS(FeatureTypeCombUptoTxVolumeDivided),1, ABS(FeatureTypeCombUptoTxVolumeDivided)) OVER (PARTITION BY FeatureTypeComb  ORDER BY  ABS(FeatureTypeCombUptoTxVolumeDivided) DESC)
				   THEN LEAD(FeatureTypeCombDivided,1) OVER (PARTITION BY FeatureTypeComb  ORDER BY  ABS(FeatureTypeCombUptoTxVolumeDivided) DESC) ELSE NULL END Lag1ClosestLowerByTxVolume


--			into ##testing
		FROM ##FeatureInteractionBehaviorsReferenceTemp -- WHERE MonthKey = xxxx
	),ABC AS
	(
	SELECT *
			--------------------------------------------------------------------------------
		--TWO PAIRED (By Tx.#)
		,CAST(Lag1ClosestLowerByTxCount AS VARCHAR(MAX))
			+ '/' +
		CAST(Lag2ByTxCount AS VARCHAR(MAX))
		Lag1ClosestLowerByTxTwoPaired
		--------------------------------------------------------------------------------
		--TWO PAIRED (By Tx.V.)
		,CAST(Lag1ClosestLowerByTxVolume AS VARCHAR(MAX))
			+ '/' +
		CAST(Lag2ByTxVolume AS VARCHAR(MAX))
		Lag1ClosestLowerByTxVTwoPaired
		
		--THREE PAIRED (By Tx.V.)
		,CAST(Lag1ClosestLowerByTxVolume AS VARCHAR(MAX))
			+ '/' +
		CAST(Lag2ByTxVolume AS VARCHAR(MAX))
		Lag1ClosestLowerByTxVThreePaired
		--------------------------------------------------------------------------------
	FROM X
	) ,A1 AS
	(
	select
		  CAST(@MainFeatureCombLastDate																								   as INT)		     MonthKey
		 ,cast(0																													   AS Tinyint)	     MeasureType
		 ,cast(0																													   AS tinyint)	     CombinationType
		 ,CAST(FeatureTypeCombDivided																									   as decimal(5,2))  UniquedFeatureType
		 ,CAST(Lag1ClosestLowerByTxCount																							   as varchar(150))  ClosenessItem
		 ,CAST(AVG(FeatureTypeCount)																									   as decimal(5,2))  AvgFeatureTypeCountInGroup
		 ,CAST(MIN(FeatureTypeCount)																									   as tinyint)	     MinFeatureTypeCountInGroup
		 ,CAST(MAX(FeatureTypeCount)																									   as tinyint)	     MaxFeatureTypeCountInGroup
		 ,CAST(COUNT(Lag1ClosestLowerByTxCount)																						   as INT)		     CountingClosestLowerByTx
		 ,CAST(SUM(UserCount)																										   as INT)		     UserCount
		 ,CAST(SUM(FeatureTypeCombUptoTxCountDivided)																					   as INT)		     SUMFeatureTypeCombUptoTxCountDivided
		 ,CAST(ABS(SUM(FeatureTypeCombUptoTxVolumeDivided))																			   as decimal(30,2)) SUMFeatureTypeCombUptoTxVolumeDivided
		 ,CAST(SUM(ABS(FeatureTypeCombUptoTxVolumeDivided)) / SUM(FeatureTypeCombUptoTxCountDivided)									   as decimal(30,2)) AvgGroupingSize
		 ,CAST(IIF(SIGN(SUM(FeatureTypeCombUptoTxVolumeDivided))=1,0,1)																   as bit)		     IsCashOutFlow
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY COUNT(Lag1ClosestLowerByTxCount) DESC)		   as INT)		     RankingByPairCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY SUM(UserCount) DESC)							   as INT)		     RankingByUserCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY SUM(FeatureTypeCombUptoTxCountDivided) DESC)	   as INT)		     RankingByTxCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY ABS(SUM(FeatureTypeCombUptoTxVolumeDivided)) DESC) as INT)		     RankingByTxVolume

	from ABC WHERE Lag1ClosestLowerByTxCount IS NOT NULL
	group by FeatureTypeCombDivided, Lag1ClosestLowerByTxCount
	),A2 AS
	(
		select
		  CAST(@MainFeatureCombLastDate																							 as INT)		   MonthKey
		 ,cast(0																												 AS tinyint)	   MeasureType
		 ,cast(1																												 AS Tinyint)	   CombinationType
		 ,CAST(FeatureTypeCombDivided																								 as decimal(5,2))  UniquedFeatureType
		 ,CAST(Lag1ClosestLowerByTxTwoPaired																					 as varchar(150))  ClosenessItem
		 ,CAST(AVG(FeatureTypeCount)																								 as decimal(5,2))  AvgFeatureTypeCountInGroup
		 ,CAST(MIN(FeatureTypeCount)																								 as tinyint)	   MinFeatureTypeCountInGroup
		 ,CAST(MAX(FeatureTypeCount)																								 as tinyint)	   MaxFeatureTypeCountInGroup
		 ,CAST(COUNT(Lag1ClosestLowerByTxTwoPaired)																				 as INT)		   CountingClosestLowerByTx
		 ,CAST(SUM(UserCount)																									 as INT)		   UserCount
		 ,CAST(SUM(FeatureTypeCombUptoTxCountDivided)																				 as INT)		   SUMFeatureTypeCombUptoTxCountDivided
		 ,CAST(ABS(SUM(FeatureTypeCombUptoTxVolumeDivided))																		 as decimal(30,2)) SUMFeatureTypeCombUptoTxVolumeDivided
		 ,CAST(SUM(ABS(FeatureTypeCombUptoTxVolumeDivided)) / SUM(FeatureTypeCombUptoTxCountDivided)								 as decimal(30,2)) AvgGroupingSize
		 ,CAST(IIF(SIGN(SUM(FeatureTypeCombUptoTxVolumeDivided))=1,0,1)															 as bit)		   IsCashOutFlow
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY COUNT(Lag1ClosestLowerByTxTwoPaired) DESC) as INT)		   RankingByPairCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY SUM(UserCount) DESC)						 as INT)		   RankingByUserCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY SUM(FeatureTypeCombUptoTxCountDivided) DESC) as INT)		   RankingByTxCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY ABS(SUM(FeatureTypeCombUptoTxVolumeDivided)) DESC) as INT)	   RankingByTxVolume
	from ABC where Lag1ClosestLowerByTxTwoPaired IS NOT NULL
	group by FeatureTypeCombDivided, Lag1ClosestLowerByTxTwoPaired
	),B1 AS
	(
	select
		  CAST(@MainFeatureCombLastDate																						  as INT)		    MonthKey
		 ,cast(1																											  AS tinyint)	    MeasureType
		 ,cast(0																											  AS Tinyint)	    CombinationType
		 ,CAST(FeatureTypeCombDivided																							  as decimal(5,2))  UniquedFeatureType
		 ,CAST(Lag1ClosestLowerByTxVolume																					  as varchar(150))  ClosenessItem
		 ,CAST(AVG(FeatureTypeCount)																							  as decimal(5,2))  AvgFeatureTypeCountInGroup
		 ,CAST(MIN(FeatureTypeCount)																							  as tinyint)	    MinFeatureTypeCountInGroup
		 ,CAST(MAX(FeatureTypeCount)																							  as tinyint)	    MaxFeatureTypeCountInGroup
		 ,CAST(COUNT(Lag1ClosestLowerByTxVolume)																			  as INT)		    CountingClosestLowerByTx
		 ,CAST(SUM(UserCount)																								  as INT)		    UserCount
		 ,CAST(SUM(FeatureTypeCombUptoTxCountDivided)																			  as INT)		    SUMFeatureTypeCombUptoTxCountDivided
		 ,CAST(ABS(SUM(FeatureTypeCombUptoTxVolumeDivided))																	  as decimal(30,2)) SUMFeatureTypeCombUptoTxVolumeDivided
		 ,CAST(SUM(ABS(FeatureTypeCombUptoTxVolumeDivided)) / SUM(FeatureTypeCombUptoTxCountDivided)							  as decimal(30,2)) AvgGroupingSize
		 ,CAST(IIF(SIGN(SUM(FeatureTypeCombUptoTxVolumeDivided))=1,0,1)														  as bit)		    IsCashOutFlow
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY COUNT(Lag1ClosestLowerByTxVolume) DESC) as INT)		    RankingByPairCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY SUM(UserCount) DESC)					  as INT)			RankingByUserCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY SUM(FeatureTypeCombUptoTxCountDivided) DESC)	   as INT)	RankingByTxCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY ABS(SUM(FeatureTypeCombUptoTxVolumeDivided)) DESC) as INT)	RankingByTxVolume
	from ABC WHERE Lag1ClosestLowerByTxVolume IS NOT NULL
	group by FeatureTypeCombDivided, Lag1ClosestLowerByTxVolume
	),B2 AS
	(
		select
		  CAST(@MainFeatureCombLastDate																							  as INT)		    MonthKey
		 ,cast(1																												  AS Tinyint)	    MeasureType
		 ,cast(1																												  AS tinyint)	    CombinationType
		 ,CAST(FeatureTypeCombDivided																								  as decimal(5,2))  UniquedFeatureType
		 ,CAST(Lag1ClosestLowerByTxVTwoPaired																					  as varchar(150))  ClosenessItem
		 ,CAST(AVG(FeatureTypeCount)																								  as decimal(5,2))  AvgFeatureTypeCountInGroup
		 ,CAST(MIN(FeatureTypeCount)																								  as tinyint)	    MinFeatureTypeCountInGroup
		 ,CAST(MAX(FeatureTypeCount)																								  as tinyint)	    MaxFeatureTypeCountInGroup
		 ,CAST(COUNT(Lag1ClosestLowerByTxVTwoPaired)																			  as INT)		    CountingClosestLowerByTx
		 ,CAST(SUM(UserCount)																									  as INT)		    UserCount
		 ,CAST(SUM(FeatureTypeCombUptoTxCountDivided)																				  as INT)		    SUMFeatureTypeCombUptoTxCountDivided
		 ,CAST(ABS(SUM(FeatureTypeCombUptoTxVolumeDivided))																		  as decimal(30,2)) SUMFeatureTypeCombUptoTxVolumeDivided
		 ,CAST(SUM(ABS(FeatureTypeCombUptoTxVolumeDivided)) / SUM(FeatureTypeCombUptoTxCountDivided)								  as decimal(30,2)) AvgGroupingSize
		 ,CAST(IIF(SIGN(SUM(FeatureTypeCombUptoTxVolumeDivided))=1,0,1)															  as bit)		    IsCashOutFlow
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY COUNT(Lag1ClosestLowerByTxVTwoPaired) DESC) as INT)		    RankingByPairCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY SUM(UserCount) DESC)						  as INT)			RankingByUserCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY SUM(FeatureTypeCombUptoTxCountDivided) DESC)  as INT)			RankingByTxCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided			 ORDER BY ABS(SUM(FeatureTypeCombUptoTxVolumeDivided)) DESC) as INT)		RankingByTxVolume
	from ABC WHERE Lag1ClosestLowerByTxVTwoPaired IS NOT NULL
	group by FeatureTypeCombDivided, Lag1ClosestLowerByTxVTwoPaired
	),C1 AS
	(
		select
		  CAST(@MainFeatureCombLastDate																						 as INT)		   MonthKey
		 ,cast(2																											 AS tinyint)	   MeasureType
		 ,cast(0																											 AS Tinyint)	   CombinationType
		 ,CAST(FeatureTypeCombDivided																							 as decimal(5,2))  UniquedFeatureType
		 ,CAST(NULL																											 as varchar(150))  ClosenessItem
		 ,CAST(AVG(FeatureTypeCount)																							 as decimal(5,2))  AvgFeatureTypeCountInGroup
		 ,CAST(MIN(FeatureTypeCount)																							 as tinyint)	   MinFeatureTypeCountInGroup
		 ,CAST(MAX(FeatureTypeCount)																							 as tinyint)	   MaxFeatureTypeCountInGroup
		 ,CAST(NULL																											 as INT)		   CountingClosestLowerByTx
		 ,CAST(SUM(UserCount)																								 as INT)		   UserCount
		 ,CAST(SUM(FeatureTypeCombUptoTxCountDivided)																			 as INT)		   SUMFeatureTypeCombUptoTxCountDivided
		 ,CAST(ABS(SUM(FeatureTypeCombUptoTxVolumeDivided))																	 as decimal(30,2)) SUMFeatureTypeCombUptoTxVolumeDivided
		 ,CAST(SUM(ABS(FeatureTypeCombUptoTxVolumeDivided)) / SUM(FeatureTypeCombUptoTxCountDivided)							 as decimal(30,2)) AvgGroupingSize
		 ,CAST(IIF(SIGN(SUM(FeatureTypeCombUptoTxVolumeDivided))=1,0,1)														 as bit)		   IsCashOutFlow
		 ,CAST(NULL																											 as INT)		   RankingByPairCount
		 ,CAST(ROW_NUMBER() OVER (ORDER BY SUM(UserCount) DESC)																 as INT)		   RankingByUserCount
		 ,CAST(ROW_NUMBER() OVER (ORDER BY SUM(FeatureTypeCombUptoTxCountDivided) DESC)										 as INT)		   RankingByTxCount
		 ,CAST(ROW_NUMBER() OVER (ORDER BY ABS(SUM(FeatureTypeCombUptoTxVolumeDivided)) DESC)									 as INT)		   RankingByTxVolume
	from ABC --where Lag1ClosestLowerByTxCount IS NOT NULL
	group by FeatureTypeCombDivided--,Lag1ClosestLowerByTxCount
	)
	insert into [DWH_DB].[dbo].[FACT_MainFeatureRelations]
	SELECT
		 MonthKey
		,MeasureType
		,CombinationType
		,UniquedFeatureType
		,ClosenessItem
	,REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(
     UniquedFeatureType,
	  '333.5.00','Precious Metal Transaction'),
	  '333.2.10','Streamer Payment (Sending)'),
	  '333.2.20','Streamer Payment (Receiving)'),
	  '333.3.00','International Money Transfer'),
	  '333.1.00','Membership Payment'),
	  '29.00','Card Purchase Fee'),
	  '28.00','Investment'),
	  '27.00','Lottery Payment'),
	  '25.00','Gift Card Topup'),
	  '24.00','Insurance'),
	  '23.00','Gift Card Payment'),
	  '22.00','Donation'),
	  '21.00','IBAN Money Transfer'),
	  '20.00','Card Money Transfer'),
	  '19.00','Saving Balance Transfer'),
	  '18.00','CityRingTravelCard Topup'),
	  '17.00','Game Payment'),
	  '16.00','Crpyto Transfer'),
	  '15.00','Cashback Reward'),
	  '1114.00','Bill Payment'),
	  '13.00','FX Transaction'),
	  '12.00','Pocket Money Transfer'),
	  '11.00','Closed Loop Payment (Canteen)'),
	  '10.00','Invitation Bonus'),
	  '222.11','Card (POS|Offline Tx.)'),
	  '222.12','Card (POS|Online Tx.)'),
	  '222.20','Card (ATM Balance Inquiry)'),
	  '222.30','Card (Card Fee)'),
	  '222.40','Card to Card Money Transfer'),
	  '222.50','Card (ATM Deposit)'),
	  '222.60','Card (Virtual Card Fee)'),
	  '222.70','Card (Montly Card Fee)'),
	  '222.80','Card (Card Fee Refund)'),
	  '222.90','Corporate Card Balance Deposit'),
	  '117.10','Closed Loop Money Transfer (Sending)'),
	  '117.20','Closed Loop Money Transfer (Receiving)'),
	  '1.10','Bank Transfer (Withdrawal)'),
	  '1.00','Bank Transfer (Deposit)'),
	  '0.00','Manual Transaction'),
	  '222.00','Card (ATM Withdraw)'),
	  '333..00','Bank/Credit Card Deposit'),
	  '4.00','Mobile Deposit'),
	  '5.00','BKM Deposit'),
	  '6.00','Cash Deposit from Physical Point'),
	  '8.00','Checkout Payment'),
	  '9.00','Mass Payment')
	  UniquedFeatureTypeNames
	,REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(
	 ClosenessItem,
	  '333.5.00','Precious Metal Transaction'),
	  '333.2.10','Streamer Payment (Sending)'),
	  '333.2.20','Streamer Payment (Receiving)'),
	  '333.3.00','International Money Transfer'),
	  '333.1.00','Membership Payment'),
	  '29.00','Card Purchase Fee'),
	  '28.00','Investment'),
	  '27.00','Lottery Payment'),
	  '25.00','Gift Card Topup'),
	  '24.00','Insurance'),
	  '23.00','Gift Card Payment'),
	  '22.00','Donation'),
	  '21.00','IBAN Money Transfer'),
	  '20.00','Card Money Transfer'),
	  '19.00','Saving Balance Transfer'),
	  '18.00','CityRingTravelCard Topup'),
	  '17.00','Game Payment'),
	  '16.00','Crpyto Transfer'),
	  '15.00','Cashback Reward'),
	  '1114.00','Bill Payment'),
	  '13.00','FX Transaction'),
	  '12.00','Pocket Money Transfer'),
	  '11.00','Closed Loop Payment (Canteen)'),
	  '10.00','Invitation Bonus'),
	  '222.11','Card (POS|Offline Tx.)'),
	  '222.12','Card (POS|Online Tx.)'),
	  '222.20','Card (ATM Balance Inquiry)'),
	  '222.30','Card (Card Fee)'),
	  '222.40','Card to Card Money Transfer'),
	  '222.50','Card (ATM Deposit)'),
	  '222.60','Card (Virtual Card Fee)'),
	  '222.70','Card (Montly Card Fee)'),
	  '222.80','Card (Card Fee Refund)'),
	  '222.90','Corporate Card Balance Deposit'),
	  '117.10','Closed Loop Money Transfer (Sending)'),
	  '117.20','Closed Loop Money Transfer (Receiving)'),
	  '1.10','Bank Transfer (Withdrawal)'),
	  '1.00','Bank Transfer (Deposit)'),
	  '0.00','Manual Transaction'),
	  '222.00','Card (ATM Withdraw)'),
	  '333..00','Bank/Credit Card Deposit'),
	  '4.00','Mobile Deposit'),
	  '5.00','BKM Deposit'),
	  '6.00','Cash Deposit from Physical Point'),
	  '8.00','Checkout Payment'),
	  '9.00','Mass Payment')
	  ClosenessItemNames
	 ,AvgFeatureTypeCountInGroup
	 ,MinFeatureTypeCountInGroup
	 ,MaxFeatureTypeCountInGroup
	 ,CountingClosestLowerByTx
	 ,UserCount
	 ,SUMFeatureTypeCombUptoTxCountDivided
	 ,SUMFeatureTypeCombUptoTxVolumeDivided
	 ,AvgGroupingSize
	 ,IsCashOutFlow
	 ,RankingByPairCount
	 ,RankingByTxCount
	 ,RankingByTxVolume
	from (
								 select*from A1 UNION select*from A2 --Tx.#
						   UNION select*from B1 UNION select*from B2 --Tx.V.
						   UNION select*from C1 --UU
		) z
drop table if exists ##FeatureInteractionBehaviorsReferenceTemp

/*TABLO BASIM SORGUSUDUR!
SELECT	A.MonthKey
		 ,FeatureTypeCount
		/*MERGERS*/			
			,A.FeatureTypeComb
			/*MERGERS (FeatureType Grouped)*/
			/*MERGERS (TRANSACTIONS [Tx.#/Tx.V./ABS(Tx.V.)*/
			,FeatureTypeCombUptoTxCount
			,FeatureTypeCombUptoTxVolume				,		FeatureTypeCombUptoAbsTxVolume	
			/*MERGERS (Averages)*/
			,AvgResultingBalancebyFeatureType			,		AvgTxVolumePerCapitaForAgg
			,AvgTxCountPerCapitaForAgg				,		AvgTicketSizeForAgg
			/*MERGERS (Rates)*/
			,TxCountRates,AbsTxVolumeRates
		/*DIVISIONS*/
			/*DIVISIONS (FeatureType Divided)*/
			,A.FeatureTypeCombDivided
			/*DIVISIONS (TRANSACTIONS [Tx.#/Tx.V./ABS(Tx.V.)*/
			,A1.FeatureTypeCombUptoTxCountDivided
			,A2.FeatureTypeCombUptoTxVolumeDivided	 ,		A3.FeatureTypeCombUptoAbsTxVolumeDivided		
			/*DIVISIONS (Averages)*/
			,A4.AvgResultingBalancebyFeatureTypeDivided,		A5.AvgTxVolumePerCapitaForAggDivided
			,A6.AvgTxCountPerCapitaForAggDivided	 ,		A7.AvgTicketSizeForAggDivided
			/*DIVISIONS (Rates)*/
			,A8.TxCountRatesDivided					 ,		A9.AbsTxVolumeRatesDivided
		/*OVERALL ATTRIBUTE CALCULATIONS*/
			/*OVERALL CALCULATIONS (TRANSACTIONS [UU/Tx.#/Tx.V./ABS(Tx.V.)*/
			,UserCount
			,AvgAge
			,SumTxCount, SumAbsTxVolume ,SumTxVolume
			/*OVERALL CALCULATIONS (AVERAGES)*/
			,AvgTicketSize							,		AvgResultingBalance
			,AvgTxCountPerCapita					,		AvgTxVolumePerCapita
		/*RANKINGS BY OVERALL CALCULATIONS*/
			,RankByUserCount						,		RankBySumTxCount
			,RankBySumAbsTxVolume					,		RankingMetricWeightedAverages
		/*PERCENTAGES BY OVERALL*/
			,UserCountOverallPercentage				,		SumTxCountOverallPercentage
			,SumAbsTxVolumeOverallPercentage		,		PercentageMetricWeightedAverages
INTO ##FeatureInteractionBehaviorsReferenceTemp
FROM
	(
			 (SELECT *						, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) irRN, CAST(VALUE AS decimal(5, 2)) FeatureTypeCombDivided				  FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(FeatureTypeComb				  ,'/') /*WHERE MonthKey = 202303*/) A
		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i1RN, CAST(VALUE AS decimal(15,2)) FeatureTypeCombUptoTxCountDivided		  FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(FeatureTypeCombUptoTxCount		  ,'/') /*WHERE MonthKey = 202303*/) A1 ON A1.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A1.i1RN AND A.MonthKey = A1.MonthKey
		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i2RN, CAST(VALUE AS decimal(15,2)) FeatureTypeCombUptoTxVolumeDivided	  FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(FeatureTypeCombUptoTxVolume	  ,'/') /*WHERE MonthKey = 202303*/) A2 ON A2.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A2.i2RN AND A.MonthKey = A2.MonthKey
		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i3RN, CAST(VALUE AS decimal(15,2)) FeatureTypeCombUptoAbsTxVolumeDivided   FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(FeatureTypeCombUptoAbsTxVolume	  ,'/') /*WHERE MonthKey = 202303*/) A3 ON A3.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A3.i3RN AND A.MonthKey = A3.MonthKey
		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i4RN, CAST(VALUE AS decimal(15,2)) AvgResultingBalancebyFeatureTypeDivided FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(AvgResultingBalancebyFeatureType ,'/') /*WHERE MonthKey = 202303*/) A4 ON A4.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A4.i4RN AND A.MonthKey = A4.MonthKey
		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i5RN, CAST(VALUE AS decimal(15,2)) AvgTxVolumePerCapitaForAggDivided	  FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(AvgTxVolumePerCapitaForAgg	  ,'/') /*WHERE MonthKey = 202303*/) A5 ON A5.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A5.i5RN AND A.MonthKey = A5.MonthKey
		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i6RN, CAST(VALUE AS decimal(15,2)) AvgTxCountPerCapitaForAggDivided	  FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(AvgTxCountPerCapitaForAgg	  ,'/') /*WHERE MonthKey = 202303*/) A6 ON A6.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A6.i6RN AND A.MonthKey = A6.MonthKey
		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i7RN, CAST(VALUE AS decimal(15,2)) AvgTicketSizeForAggDivided			  FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(AvgTicketSizeForAgg			  ,'/') /*WHERE MonthKey = 202303*/) A7 ON A7.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A7.i7RN AND A.MonthKey = A7.MonthKey
		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i8RN, CAST(VALUE AS decimal(15,2)) TxCountRatesDivided					  FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(TxCountRates					  ,'/') /*WHERE MonthKey = 202303*/) A8 ON A8.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A8.i8RN AND A.MonthKey = A8.MonthKey
		JOIN (SELECT MonthKey, FeatureTypeComb, ROW_NUMBER() OVER (PARTITION BY FeatureTypeComb,MonthKey order by (SELECT NULL)) i9RN, CAST(VALUE AS decimal(15,2)) AbsTxVolumeRatesDivided				  FROM FACT_MainFeatureCombinations with (NOLOCK) CROSS APPLY STRING_SPLIT(AbsTxVolumeRates				  ,'/') /*WHERE MonthKey = 202303*/) A9 ON A9.FeatureTypeComb = A.FeatureTypeComb AND A.irRN = A9.i9RN AND A.MonthKey = A9.MonthKey
	)
;
WITH X  AS
	(
		SELECT 
			  DISTINCT
			  MonthKey
			 ,FeatureTypeComb
			 ,FeatureTypeCombDivided
			 ,UserCount
			 ,AvgAge
			 ,FeatureTypeCombUptoTxCountDivided
			 ,FeatureTypeCombUptoTxVolumeDivided
			 ,FeatureTypeCombUptoAbsTxVolumeDivided
			 ,AvgResultingBalancebyFeatureTypeDivided
			 ,AvgTxVolumePerCapitaForAggDivided
			 ,AvgTxCountPerCapitaForAggDivided
			 ,AvgTicketSizeForAggDivided
			 ,TxCountRatesDivided
			 ,AbsTxVolumeRatesDivided
			 ,COUNT(*)								   OVER (PARTITION BY FeatureTypeCombDivided, MonthKey) CountFeatureTypeCombDivided
			 ,SUM(FeatureTypeCombUptoTxCountDivided)	   OVER (PARTITION BY FeatureTypeCombDivided, MonthKey) SumFeatureTypeCombUptoTxCountDivided
			 ,SUM(FeatureTypeCombUptoAbsTxVolumeDivided) OVER (PARTITION BY FeatureTypeCombDivided, MonthKey) SumFeatureTypeCombUptoAbsTxVolumeDivided
			,FeatureTypeCount
		/*Tx Count*/


			,CASE WHEN MAX(FeatureTypeCombUptoTxCountDivided) OVER (PARTITION BY FeatureTypeComb, MonthKey ORDER BY FeatureTypeCombUptoTxCountDivided DESC) 
						>= LEAD(FeatureTypeCombUptoTxCountDivided,1,FeatureTypeCombUptoTxCountDivided) OVER (PARTITION BY FeatureTypeComb, MonthKey  ORDER BY FeatureTypeCombUptoTxCountDivided DESC)
				   THEN LEAD(FeatureTypeCombDivided,2) OVER (PARTITION BY FeatureTypeComb, MonthKey  ORDER BY FeatureTypeCombUptoTxCountDivided DESC) ELSE NULL END Lag2ByTxCount

			,CASE WHEN MAX(FeatureTypeCombUptoTxCountDivided) OVER (PARTITION BY FeatureTypeComb, MonthKey ORDER BY FeatureTypeCombUptoTxCountDivided DESC) 
						>= LEAD(FeatureTypeCombUptoTxCountDivided,1,FeatureTypeCombUptoTxCountDivided) OVER (PARTITION BY FeatureTypeComb, MonthKey  ORDER BY FeatureTypeCombUptoTxCountDivided DESC)
				   THEN LEAD(FeatureTypeCombDivided,1) OVER (PARTITION BY FeatureTypeComb, MonthKey  ORDER BY FeatureTypeCombUptoTxCountDivided DESC) ELSE NULL END Lag1ClosestLowerByTxCount

		/*Tx. Volume*/


			,CASE WHEN MAX(ABS(FeatureTypeCombUptoTxVolumeDivided)) OVER (PARTITION BY FeatureTypeComb, MonthKey ORDER BY  ABS(FeatureTypeCombUptoTxVolumeDivided) DESC) 
						>= LEAD( ABS(FeatureTypeCombUptoTxVolumeDivided),1, ABS(FeatureTypeCombUptoTxVolumeDivided)) OVER (PARTITION BY FeatureTypeComb, MonthKey  ORDER BY  ABS(FeatureTypeCombUptoTxVolumeDivided) DESC)
				   THEN LEAD(FeatureTypeCombDivided,2) OVER (PARTITION BY FeatureTypeComb, MonthKey  ORDER BY  ABS(FeatureTypeCombUptoTxVolumeDivided) DESC) ELSE NULL END Lag2ByTxVolume


			,CASE WHEN MAX(ABS(FeatureTypeCombUptoTxVolumeDivided)) OVER (PARTITION BY FeatureTypeComb, MonthKey ORDER BY  ABS(FeatureTypeCombUptoTxVolumeDivided) DESC) 
						>= LEAD( ABS(FeatureTypeCombUptoTxVolumeDivided),1, ABS(FeatureTypeCombUptoTxVolumeDivided)) OVER (PARTITION BY FeatureTypeComb, MonthKey  ORDER BY  ABS(FeatureTypeCombUptoTxVolumeDivided) DESC)
				   THEN LEAD(FeatureTypeCombDivided,1) OVER (PARTITION BY FeatureTypeComb, MonthKey  ORDER BY  ABS(FeatureTypeCombUptoTxVolumeDivided) DESC) ELSE NULL END Lag1ClosestLowerByTxVolume


--			into ##testing
		FROM ##FeatureInteractionBehaviorsReferenceTemp -- WHERE MonthKey = xxxx
	),ABC AS
	(
	SELECT *
			--------------------------------------------------------------------------------
		--TWO PAIRED (By Tx.#)
		,CAST(Lag1ClosestLowerByTxCount AS VARCHAR(MAX))
			+ '/' +
		CAST(Lag2ByTxCount AS VARCHAR(MAX))
		Lag1ClosestLowerByTxTwoPaired
		--------------------------------------------------------------------------------
		--TWO PAIRED (By Tx.V.)
		,CAST(Lag1ClosestLowerByTxVolume AS VARCHAR(MAX))
			+ '/' +
		CAST(Lag2ByTxVolume AS VARCHAR(MAX))
		Lag1ClosestLowerByTxVTwoPaired
		
		--THREE PAIRED (By Tx.V.)
		,CAST(Lag1ClosestLowerByTxVolume AS VARCHAR(MAX))
			+ '/' +
		CAST(Lag2ByTxVolume AS VARCHAR(MAX))
		Lag1ClosestLowerByTxVThreePaired
		--------------------------------------------------------------------------------
	FROM X
	) ,A1 AS
	(
	select
		  CAST(MonthKey																												   as INT)		     MonthKey
		 ,cast(0																													   AS Tinyint)	     MeasureType
		 ,cast(0																													   AS tinyint)	     CombinationType
		 ,CAST(FeatureTypeCombDivided																									   as decimal(5,2))  UniquedFeatureType
		 ,CAST(Lag1ClosestLowerByTxCount																							   as varchar(150))  ClosenessItem
		 ,CAST(AVG(FeatureTypeCount)																									   as decimal(5,2))  AvgFeatureTypeCountInGroup
		 ,CAST(MIN(FeatureTypeCount)																									   as tinyint)	     MinFeatureTypeCountInGroup
		 ,CAST(MAX(FeatureTypeCount)																									   as tinyint)	     MaxFeatureTypeCountInGroup
		 ,CAST(COUNT(Lag1ClosestLowerByTxCount)																						   as INT)		     CountingClosestLowerByTx
		 ,CAST(SUM(UserCount)																										   as INT)		     UserCount
		 ,CAST(SUM(FeatureTypeCombUptoTxCountDivided)																					   as INT)		     SUMFeatureTypeCombUptoTxCountDivided
		 ,CAST(ABS(SUM(FeatureTypeCombUptoTxVolumeDivided))																			   as decimal(30,2)) SUMFeatureTypeCombUptoTxVolumeDivided
		 ,CAST(SUM(ABS(FeatureTypeCombUptoTxVolumeDivided)) / SUM(FeatureTypeCombUptoTxCountDivided)									   as decimal(30,2)) AvgGroupingSize
		 ,CAST(IIF(SIGN(SUM(FeatureTypeCombUptoTxVolumeDivided))=1,0,1)																   as bit)		     IsCashOutFlow
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY COUNT(Lag1ClosestLowerByTxCount) DESC)		   as INT)		     RankingByPairCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY SUM(UserCount) DESC)							   as INT)		     RankingByUserCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY SUM(FeatureTypeCombUptoTxCountDivided) DESC)	   as INT)		     RankingByTxCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY ABS(SUM(FeatureTypeCombUptoTxVolumeDivided)) DESC) as INT)		     RankingByTxVolume

	from ABC WHERE Lag1ClosestLowerByTxCount IS NOT NULL
	group by MonthKey,FeatureTypeCombDivided, Lag1ClosestLowerByTxCount
	),A2 AS
	(
		select
		  CAST(MonthKey																											 as INT)		   MonthKey
		 ,cast(0																												 AS tinyint)	   MeasureType
		 ,cast(1																												 AS Tinyint)	   CombinationType
		 ,CAST(FeatureTypeCombDivided																								 as decimal(5,2))  UniquedFeatureType
		 ,CAST(Lag1ClosestLowerByTxTwoPaired																					 as varchar(150))  ClosenessItem
		 ,CAST(AVG(FeatureTypeCount)																								 as decimal(5,2))  AvgFeatureTypeCountInGroup
		 ,CAST(MIN(FeatureTypeCount)																								 as tinyint)	   MinFeatureTypeCountInGroup
		 ,CAST(MAX(FeatureTypeCount)																								 as tinyint)	   MaxFeatureTypeCountInGroup
		 ,CAST(COUNT(Lag1ClosestLowerByTxTwoPaired)																				 as INT)		   CountingClosestLowerByTx
		 ,CAST(SUM(UserCount)																									 as INT)		   UserCount
		 ,CAST(SUM(FeatureTypeCombUptoTxCountDivided)																				 as INT)		   SUMFeatureTypeCombUptoTxCountDivided
		 ,CAST(ABS(SUM(FeatureTypeCombUptoTxVolumeDivided))																		 as decimal(30,2)) SUMFeatureTypeCombUptoTxVolumeDivided
		 ,CAST(SUM(ABS(FeatureTypeCombUptoTxVolumeDivided)) / SUM(FeatureTypeCombUptoTxCountDivided)								 as decimal(30,2)) AvgGroupingSize
		 ,CAST(IIF(SIGN(SUM(FeatureTypeCombUptoTxVolumeDivided))=1,0,1)															 as bit)		   IsCashOutFlow
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY COUNT(Lag1ClosestLowerByTxTwoPaired) DESC) as INT)		   RankingByPairCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY SUM(UserCount) DESC)						 as INT)		   RankingByUserCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY SUM(FeatureTypeCombUptoTxCountDivided) DESC)	   as INT)	   RankingByTxCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY ABS(SUM(FeatureTypeCombUptoTxVolumeDivided)) DESC) as INT)	   RankingByTxVolume
	from ABC where Lag1ClosestLowerByTxTwoPaired IS NOT NULL
	group by MonthKey,FeatureTypeCombDivided, Lag1ClosestLowerByTxTwoPaired
	),B1 AS
	(
	select
		  CAST(MonthKey																										  as INT)		    MonthKey
		 ,cast(1																											  AS tinyint)	    MeasureType
		 ,cast(0																											  AS Tinyint)	    CombinationType
		 ,CAST(FeatureTypeCombDivided																							  as decimal(5,2))  UniquedFeatureType
		 ,CAST(Lag1ClosestLowerByTxVolume																					  as varchar(150))  ClosenessItem
		 ,CAST(AVG(FeatureTypeCount)																							  as decimal(5,2))  AvgFeatureTypeCountInGroup
		 ,CAST(MIN(FeatureTypeCount)																							  as tinyint)	    MinFeatureTypeCountInGroup
		 ,CAST(MAX(FeatureTypeCount)																							  as tinyint)	    MaxFeatureTypeCountInGroup
		 ,CAST(COUNT(Lag1ClosestLowerByTxVolume)																			  as INT)		    CountingClosestLowerByTx
		 ,CAST(SUM(UserCount)																								  as INT)		    UserCount
		 ,CAST(SUM(FeatureTypeCombUptoTxCountDivided)																			  as INT)		    SUMFeatureTypeCombUptoTxCountDivided
		 ,CAST(ABS(SUM(FeatureTypeCombUptoTxVolumeDivided))																	  as decimal(30,2)) SUMFeatureTypeCombUptoTxVolumeDivided
		 ,CAST(SUM(ABS(FeatureTypeCombUptoTxVolumeDivided)) / SUM(FeatureTypeCombUptoTxCountDivided)							  as decimal(30,2)) AvgGroupingSize
		 ,CAST(IIF(SIGN(SUM(FeatureTypeCombUptoTxVolumeDivided))=1,0,1)														  as bit)		    IsCashOutFlow
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY COUNT(Lag1ClosestLowerByTxVolume) DESC) as INT)		    RankingByPairCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY SUM(UserCount) DESC)					  as INT)			RankingByUserCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY SUM(FeatureTypeCombUptoTxCountDivided) DESC)	   as INT)	RankingByTxCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY ABS(SUM(FeatureTypeCombUptoTxVolumeDivided)) DESC) as INT)	RankingByTxVolume
	from ABC WHERE Lag1ClosestLowerByTxVolume IS NOT NULL
	group by MonthKey,FeatureTypeCombDivided, Lag1ClosestLowerByTxVolume
	),B2 AS
	(
		select
		  CAST(MonthKey																											  as INT)		    MonthKey
		 ,cast(1																												  AS Tinyint)	    MeasureType
		 ,cast(1																												  AS tinyint)	    CombinationType
		 ,CAST(FeatureTypeCombDivided																								  as decimal(5,2))  UniquedFeatureType
		 ,CAST(Lag1ClosestLowerByTxVTwoPaired																					  as varchar(150))  ClosenessItem
		 ,CAST(AVG(FeatureTypeCount)																								  as decimal(5,2))  AvgFeatureTypeCountInGroup
		 ,CAST(MIN(FeatureTypeCount)																								  as tinyint)	    MinFeatureTypeCountInGroup
		 ,CAST(MAX(FeatureTypeCount)																								  as tinyint)	    MaxFeatureTypeCountInGroup
		 ,CAST(COUNT(Lag1ClosestLowerByTxVTwoPaired)																			  as INT)		    CountingClosestLowerByTx
		 ,CAST(SUM(UserCount)																									  as INT)		    UserCount
		 ,CAST(SUM(FeatureTypeCombUptoTxCountDivided)																				  as INT)		    SUMFeatureTypeCombUptoTxCountDivided
		 ,CAST(ABS(SUM(FeatureTypeCombUptoTxVolumeDivided))																		  as decimal(30,2)) SUMFeatureTypeCombUptoTxVolumeDivided
		 ,CAST(SUM(ABS(FeatureTypeCombUptoTxVolumeDivided)) / SUM(FeatureTypeCombUptoTxCountDivided)								  as decimal(30,2)) AvgGroupingSize
		 ,CAST(IIF(SIGN(SUM(FeatureTypeCombUptoTxVolumeDivided))=1,0,1)															  as bit)		    IsCashOutFlow
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY COUNT(Lag1ClosestLowerByTxVTwoPaired) DESC) as INT)		    RankingByPairCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY SUM(UserCount) DESC)						   as INT)			RankingByUserCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY SUM(FeatureTypeCombUptoTxCountDivided) DESC)	   as INT)		RankingByTxCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY FeatureTypeCombDivided,MonthKey ORDER BY ABS(SUM(FeatureTypeCombUptoTxVolumeDivided)) DESC) as INT)		RankingByTxVolume
	from ABC WHERE Lag1ClosestLowerByTxVTwoPaired IS NOT NULL
	group by MonthKey,FeatureTypeCombDivided, Lag1ClosestLowerByTxVTwoPaired
	),C1 AS
	(
		select
		  CAST(MonthKey																										 as INT)		   MonthKey
		 ,cast(2																											 AS tinyint)	   MeasureType
		 ,cast(0																											 AS Tinyint)	   CombinationType
		 ,CAST(FeatureTypeCombDivided																							 as decimal(5,2))  UniquedFeatureType
		 ,CAST(NULL																											 as varchar(150))  ClosenessItem
		 ,CAST(AVG(FeatureTypeCount)																							 as decimal(5,2))  AvgFeatureTypeCountInGroup
		 ,CAST(MIN(FeatureTypeCount)																							 as tinyint)	   MinFeatureTypeCountInGroup
		 ,CAST(MAX(FeatureTypeCount)																							 as tinyint)	   MaxFeatureTypeCountInGroup
		 ,CAST(NULL																											 as INT)		   CountingClosestLowerByTx
		 ,CAST(SUM(UserCount)																								 as INT)		   UserCount
		 ,CAST(SUM(FeatureTypeCombUptoTxCountDivided)																			 as INT)		   SUMFeatureTypeCombUptoTxCountDivided
		 ,CAST(ABS(SUM(FeatureTypeCombUptoTxVolumeDivided))																	 as decimal(30,2)) SUMFeatureTypeCombUptoTxVolumeDivided
		 ,CAST(SUM(ABS(FeatureTypeCombUptoTxVolumeDivided)) / SUM(FeatureTypeCombUptoTxCountDivided)							 as decimal(30,2)) AvgGroupingSize
		 ,CAST(IIF(SIGN(SUM(FeatureTypeCombUptoTxVolumeDivided))=1,0,1)														 as bit)		   IsCashOutFlow
		 ,CAST(NULL																											 as INT)		   RankingByPairCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY MonthKey ORDER BY SUM(UserCount) DESC)										 as INT)		   RankingByUserCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY MonthKey ORDER BY SUM(FeatureTypeCombUptoTxCountDivided) DESC)					 as INT)		   RankingByTxCount
		 ,CAST(ROW_NUMBER() OVER (PARTITION BY MonthKey ORDER BY ABS(SUM(FeatureTypeCombUptoTxVolumeDivided)) DESC)			 as INT)		   RankingByTxVolume
	from ABC --where Lag1ClosestLowerByTxCount IS NOT NULL
	group by MonthKey,FeatureTypeCombDivided--,Lag1ClosestLowerByTxCount
	)
	insert into BI_Workspace..FACT_MainFeatureRelations
	SELECT
		 MonthKey
		,MeasureType
		,CombinationType
		,UniquedFeatureType
		,ClosenessItem
	,REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     REPLACE(REPLACE(REPLACE(REPLACE(
     UniquedFeatureType
	 ,'333.3.00','International Money Transfer'),
	  '333.2.00','Streamer Money Transfer'),
	  '333.1.00','Membership Payment'),
	  '29.00','Card Purchase Fee'),
	  '28.00','Investment'),
	  '27.00','Lottery Payment'),
	  '25.00','Gift Card Topup'),
	  '24.00','Insurance'),
	  '23.00','Gift Card Payment'),
	  '22.00','Donation'),
	  '21.00','IBAN Money Transfer'),
	  '20.00','Card Money Transfer'),
	  '19.00','Saving Balance Transfer'),
	  '18.00','CityRingTravelCard Topup'),
	  '17.00','Game Payment'),
	  '16.00','Crpyto Transfer'),
	  '15.00','Cashback Reward'),
	  '1114.00','Bill Payment'),
	  '13.00','FX Transaction'),
	  '12.00','Pocket Money Transfer'),
	  '11.00','Closed Loop Payment (Canteen)'),
	  '10.00','Invitation Bonus'),
	  '222.11','Card (POS|Offline Tx.)'),
	  '222.12','Card (POS|Online Tx.)'),
	  '222.20','Card (ATM Balance Inquiry)'),
	  '222.30','Card (Card Fee)'),
	  '222.40','Card to Card Money Transfer'),
	  '222.50','Card (ATM Deposit)'),
	  '222.60','Card (Virtual Card Fee)'),
	  '222.70','Card (Montly Card Fee)'),
	  '222.80','Card (Card Fee Refund)'),
	  '222.90','Corporate Card Balance Deposit'),
	  '117.10','Closed Loop Money Transfer (Sending)'),
	  '117.20','Closed Loop Money Transfer (Receiving)'),
	  '1.10','Bank Transfer (Withdrawal)'),
	  '1.00','Bank Transfer (Deposit)'),
	  '0.00','Manual Transaction'),
	  '222.00','Card (ATM Withdraw)'),
	  '333..00','Bank/Credit Card Deposit'),
	  '4.00','Mobile Deposit'),
	  '5.00','BKM Deposit'),
	  '6.00','Cash Deposit from Physical Point'),
	  '8.00','Checkout Payment'),
	  '9.00','Mass Payment')
	  UniquedFeatureTypeNames
	,REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 REPLACE(REPLACE(REPLACE(REPLACE(
	 ClosenessItem
	 ,'333.3.00','International Money Transfer'),
	  '333.2.00','Streamer Money Transfer'),
	  '333.1.00','Membership Payment'),
	  '29.00','Card Purchase Fee'),
	  '28.00','Investment'),
	  '27.00','Lottery Payment'),
	  '25.00','Gift Card Topup'),
	  '24.00','Insurance'),
	  '23.00','Gift Card Payment'),
	  '22.00','Donation'),
	  '21.00','IBAN Money Transfer'),
	  '20.00','Card Money Transfer'),
	  '19.00','Saving Balance Transfer'),
	  '18.00','CityRingTravelCard Topup'),
	  '17.00','Game Payment'),
	  '16.00','Crpyto Transfer'),
	  '15.00','Cashback Reward'),
	  '1114.00','Bill Payment'),
	  '13.00','FX Transaction'),
	  '12.00','Pocket Money Transfer'),
	  '11.00','Closed Loop Payment (Canteen)'),
	  '10.00','Invitation Bonus'),
	  '222.11','Card (POS|Offline Tx.)'),
	  '222.12','Card (POS|Online Tx.)'),
	  '222.20','Card (ATM Balance Inquiry)'),
	  '222.30','Card (Card Fee)'),
	  '222.40','Card to Card Money Transfer'),
	  '222.50','Card (ATM Deposit)'),
	  '222.60','Card (Virtual Card Fee)'),
	  '222.70','Card (Montly Card Fee)'),
	  '222.80','Card (Card Fee Refund)'),
	  '222.90','Corporate Card Balance Deposit'),
	  '117.10','Closed Loop Money Transfer (Sending)'),
	  '117.20','Closed Loop Money Transfer (Receiving)'),
	  '1.10','Bank Transfer (Withdrawal)'),
	  '1.00','Bank Transfer (Deposit)'),
	  '0.00','Manual Transaction'),
	  '222.00','Card (ATM Withdraw)'),
	  '333..00','Bank/Credit Card Deposit'),
	  '4.00','Mobile Deposit'),
	  '5.00','BKM Deposit'),
	  '6.00','Cash Deposit from Physical Point'),
	  '8.00','Checkout Payment'),
	  '9.00','Mass Payment')
	  ClosenessItemNames
	 ,AvgFeatureTypeCountInGroup
	 ,MinFeatureTypeCountInGroup
	 ,MaxFeatureTypeCountInGroup
	 ,CountingClosestLowerByTx
	 ,UserCount
	 ,SUMFeatureTypeCombUptoTxCountDivided
	 ,SUMFeatureTypeCombUptoTxVolumeDivided
	 ,AvgGroupingSize
	 ,IsCashOutFlow
	 ,RankingByPairCount
	 ,RankingByTxCount
	 ,RankingByTxVolume
--	into BI_Workspace..FACT_MainFeatureRelations 
	from (
								 select*from A1 UNION select*from A2 --Tx.#
						   UNION select*from B1 UNION select*from B2 --Tx.V.
						   UNION select*from C1 --UU
		) z
*/