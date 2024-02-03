class ShOm_COSTData {
    constructor() {
        this.ShOm_joinCost = 0;
        this.ShOm_isReverseOrder = false;
        this.ShOm_joinType = '';
    }
}

class ShOm_QueryProcessor {
    static ShOm_ShOm_bufferMemoryWithBuff = 50;
    static ShOm_ShOm_bufferMemoryWithlessBuff = 30;
    static ShOm_t1TupPerPage;
    static ShOm_t2TupPerPage;
    static ShOm_t3TupPerPage;
    static ShOm_pageSize = 4096;
    static ShOm_blockSize = 100;
    static ShOm_output = '';
    static ShOm_isSortergeDone = false;
    static ShOm_t1s = 20;
    static ShOm_t1p = 1000;
    static ShOm_t2s = 40;
    static ShOm_t2p = 500;
    static ShOm_t3s = 100;
    static ShOm_t3p = 2000;
    static ShOm_Q1t = "Join t1 t2\r\n" + "Join temp1 t3\r\n" + "Project temp2\r\n" + "GroupBy temp3";
    static RShOm_Q1t = "Join t1 t3\r\n" + "GroupBy temp0\r\n" + "Join t1 Temp1\r\n" + "Join t2 Temp2\r\n"
        + "Project temp3\r\n" + "GroupBy temp3";

    static main() {
        ShOm_QueryProcessor.ShOm_processTheQuery(ShOm_QueryProcessor.ShOm_t1s, ShOm_QueryProcessor.ShOm_t1p, ShOm_QueryProcessor.ShOm_t2s, ShOm_QueryProcessor.ShOm_t2p, ShOm_QueryProcessor.ShOm_t3s, ShOm_QueryProcessor.ShOm_t3p);
    }

    static ShOm_processTheQuery(ShOm_t1s, ShOm_t1pages, ShOm_t2s, ShOm_t2pages, ShOm_t3s, ShOm_t3pages) {
        ShOm_QueryProcessor.ShOm_t1TupPerPage = ShOm_QueryProcessor.ShOm_pageSize / ShOm_t1s;
        ShOm_QueryProcessor.ShOm_t2TupPerPage = ShOm_QueryProcessor.ShOm_pageSize / ShOm_t2s;
        ShOm_QueryProcessor.ShOm_t3TupPerPage = ShOm_QueryProcessor.ShOm_pageSize / ShOm_t3s;
        console.log("Go with Q1 query");
        let cost1 = ShOm_QueryProcessor.ShOm_processQ1(ShOm_t1pages, ShOm_t2pages, ShOm_t3pages);
        console.log("Go with RQ1 query");
        let cost2 = ShOm_QueryProcessor.ShOm_processRQ1(ShOm_t1pages, ShOm_t2pages, ShOm_t3pages);
        ShOm_QueryProcessor.displayShOm_output(cost1, cost2);
    }

    static displayShOm_output(cost1, cost2) {
        ShOm_QueryProcessor.ShOm_output += "\n---Best Query Plan:---\n";
        try {
            if (cost1 > cost2) {
                ShOm_QueryProcessor.ShOm_output += "\nThe Best Query Plan is RQ1 because cost of RQ1 is less than cost of Q1\n";
                console.log(ShOm_QueryProcessor.ShOm_output);
            } else {
                ShOm_QueryProcessor.ShOm_output += "\nThe Best Query Plan is Q1 since cost of RQ1 is more than cost of Q1\n";
                console.log(ShOm_QueryProcessor.ShOm_output);
            }
        } catch (ex) {
        }
    }

    static ShOm_findMinimumJoinCost(leftTable, rightTable, obj, type) {
        try {
            let cost = 0;

            if (type === 1)
                cost = ShOm_QueryProcessor.ShOm_TNLJoinCost(leftTable, rightTable, ShOm_QueryProcessor.ShOm_t1TupPerPage, obj);
            else if (type === 2)
                cost = ShOm_QueryProcessor.ShOm_TNLJoinCost(leftTable, rightTable, ShOm_QueryProcessor.ShOm_t3TupPerPage, obj);
            else if (type === 3)
                cost = ShOm_QueryProcessor.ShOm_TNLJoinCost(leftTable, rightTable, ShOm_QueryProcessor.ShOm_t1TupPerPage, obj);
            else if (type === 4)
                cost = ShOm_QueryProcessor.ShOm_TNLJoinCost(leftTable, rightTable, ShOm_QueryProcessor.ShOm_t1TupPerPage, obj);
            else if (type === 5)
                cost = ShOm_QueryProcessor.ShOm_TNLJoinCost(leftTable, rightTable, ShOm_QueryProcessor.ShOm_t2TupPerPage, obj);
            cost = ShOm_QueryProcessor.ShOm_PNLJoinCost(leftTable, rightTable, obj);

            cost = ShOm_QueryProcessor.ShOm_hashJoinCostWithBuffer(leftTable, rightTable, obj);
            cost = ShOm_QueryProcessor.ShOm_hashJoinCostWithLessBuffer(leftTable, rightTable, obj);
            cost = ShOm_QueryProcessor.SShOm_MJJoinCostWithBuf(leftTable, rightTable, obj);
            cost = ShOm_QueryProcessor.ShOm_SMJJoinCostWithLessBuf(leftTable, rightTable, obj);
            cost = ShOm_QueryProcessor.ShOm_BNLJoinCostWithBuf(leftTable, rightTable, obj);
            cost = ShOm_QueryProcessor.ShOm_BNLJoinCostWithLessBuf(leftTable, rightTable, obj);
        } catch (ex) {
        }
    }

    static ShOm_GetGroupByCost(table1, table2, isSMJDone) {
        try {
            if (isSMJDone) {
                return 2 * (table1 + table2);
            } else {
                return 3 * (table1 + table2);
            }
        } catch (ex) {
        }
        return 0;
    }

    static ShOm_joinTables(table1, table2) {
        return table1 * table2;
    }

    static ShOm_QueryProcessoringCost(diskIOCost) {
        try {
            let ioCost = BigInt(diskIOCost) * BigInt(12) / BigInt(1000);
            let totalSeconds = parseInt(ioCost);
            let seconds = totalSeconds % 60;
            let totalMinutes = totalSeconds / 60;
            let minutes = totalMinutes % 60;
            let hours = totalMinutes / 60;
            return hours + " HR(S) " + minutes + " MIN(S) " + seconds + " SEC(S)";
        } catch (ex) {
        }
        return "";
    }

    static ShOm_processQ1(ShOm_t1pages, ShOm_t2pages, ShOm_t3pages) {
        try {
            ShOm_QueryProcessor.ShOm_output += "\n---Cost of Q1:---\n";
            let totalCost = 0;
            let tempTable = 0;
            for (let line of ShOm_QueryProcessor.ShOm_Q1t.split("\r\n")) {
                if (line.toLowerCase().includes("join")) {
                    let elements = line.split(" ");
                    let leftTable = elements[1];
                    let rightTable = elements[2];
                    if (leftTable === "t1" && rightTable === "t2") {
                        if (ShOm_t1pages > ShOm_t2pages) {
                            let select = new ShOm_COSTData();
                            ShOm_QueryProcessor.ShOm_findMinimumJoinCost(ShOm_t2pages, ShOm_t1pages, select, 1);
                            tempTable = ShOm_QueryProcessor.ShOm_joinTables(ShOm_t1pages, ShOm_t2pages);
                            tempTable = (tempTable * 15) / 100;
                            console.log("The temp value is " + tempTable);
                            totalCost += select.joinCost;
                            ShOm_QueryProcessor.ShOm_output += elements[0];
                            if (select.isReverseOrder) {
                                ShOm_QueryProcessor.ShOm_output += " " + elements[2] + " " + elements[1];
                            } else {
                                ShOm_QueryProcessor.ShOm_output += " " + elements[1] + " " + elements[2];
                            }
                            ShOm_QueryProcessor.ShOm_output += "-->Cost: " + select.joinCost + ", Join type:  " + select.joinType + "\n";
                        }
                    } else if (leftTable === "temp1") {
                                                let select2 = new ShOm_COSTData();
                        ShOm_QueryProcessor.ShOm_TNLJoinCost(ShOm_t3pages, tempTable, ShOm_QueryProcessor.ShOm_t3TupPerPage, select2);
                        tempTable = (tempTable * 10 * 20) / 10000;
                        totalCost += select2.joinCost;
                        ShOm_QueryProcessor.ShOm_output += elements[0];
                        if (select2.isReverseOrder) {
                            ShOm_QueryProcessor.ShOm_output += " " + elements[2] + " " + elements[1];
                        } else {
                            ShOm_QueryProcessor.ShOm_output += " " + elements[1] + " " + elements[2];
                        }
                        ShOm_QueryProcessor.ShOm_output += "-->Cost: " + select2.joinCost + ", Join type: " + select2.joinType + "\n";
                    }
                }
                if (line.toLowerCase().includes("project")) {
                    let elements = line.split(" ");
                    ShOm_QueryProcessor.ShOm_output += elements[0] + " " + elements[1] + "-->Cost is too small, we will neglect it.\n";
                }
                if (line.toLowerCase().includes("groupby")) {
                    let elements = line.split(" ");
                    let groupByCost = ShOm_QueryProcessor.ShOm_GetGroupByCost(tempTable, ShOm_t3pages, false);
                    totalCost += groupByCost;
                    ShOm_QueryProcessor.ShOm_output += elements[0] + " " + elements[1] + "-->Cost: " + groupByCost + "\n";
                }
            }
            ShOm_QueryProcessor.ShOm_output += "Total Disk I/O Cost is :- " + totalCost + "\n";
            ShOm_QueryProcessor.ShOm_output += "Query Processing Cost :- " + ShOm_QueryProcessor.ShOm_QueryProcessoringCost(totalCost) + "\n";
            return totalCost;
        } catch (ex) {
        }
        return 0;
    }

    static ShOm_processRQ1(ShOm_t1pages, ShOm_t2pages, ShOm_t3pages) {
        try {
            ShOm_QueryProcessor.ShOm_output += "\n---Cost of RQ1--- :- \n";
            let totalShOm_QueryProcessoringCost = 0;
            let tempTable = 0;
            for (let line of ShOm_QueryProcessor.RShOm_Q1t.split("\r\n")) {
                if (line.toLowerCase().includes("join")) {
                    let elements = line.split(" ");
                    let leftTable = elements[1];
                    let rightTable = elements[2];
                    if (leftTable === "t1" && rightTable === "t3") {
                        if (ShOm_t3pages > ShOm_t1pages) {
                            let select = new ShOm_COSTData();
                            ShOm_QueryProcessor.ShOm_findMinimumJoinCost(ShOm_t3pages, ShOm_t1pages, select, 3);
                            tempTable = ShOm_QueryProcessor.ShOm_joinTables(ShOm_t3pages, ShOm_t1pages);
                            tempTable = (tempTable * 20) / 100;
                            totalShOm_QueryProcessoringCost += select.joinCost;
                            if (select.isReverseOrder) {
                                ShOm_QueryProcessor.ShOm_output += elements[0] + " " + elements[2] + " " + elements[1];
                            } else {
                                ShOm_QueryProcessor.ShOm_output += elements[0] + " " + elements[1] + " " + elements[2];
                            }
                            ShOm_QueryProcessor.ShOm_output += "-->Cost: " + select.joinCost + ", Join type: " + select.joinType + "\n";
                        }
                    } else if (leftTable === "t1") {
                        let select1 = new ShOm_COSTData();
                        ShOm_QueryProcessor.ShOm_findMinimumJoinCost(ShOm_t1pages, tempTable, select1, 4);
                        tempTable = (tempTable * 15) / 100;
                        totalShOm_QueryProcessoringCost += select1.joinCost;
                        if (select1.isReverseOrder) {
                            ShOm_QueryProcessor.ShOm_output += elements[0] + " " + elements[2] + " " + elements[1];
                        } else {
                            ShOm_QueryProcessor.ShOm_output += elements[0] + " " + elements[1] + " " + elements[2];
                        }
                        ShOm_QueryProcessor.ShOm_output += "-->Cost: " + select1.joinCost + ", Join type: " + select1.joinType + "\n";
                    } else if (leftTable === "t2") {
                        let select2 = new ShOm_COSTData();
                        ShOm_QueryProcessor.ShOm_findMinimumJoinCost(ShOm_t2pages, tempTable, select2, 5);
                        tempTable = (tempTable * 10) / 100;
                        totalShOm_QueryProcessoringCost += select2.joinCost;
                        if (select2.isReverseOrder) {
                            ShOm_QueryProcessor.ShOm_output += elements[0] + " " + elements[2] + " " + elements[1] + "-->Cost: "
                                + select2.joinCost + ", Join type: " + select2.joinType + "" + "\n";
                        } else {
                            ShOm_QueryProcessor.ShOm_output += elements[0] + " " + elements[1] + " " + elements[2] + "-->Cost: "
                                + select2.joinCost + ", Join type: " + select2.joinType + "\n";
                        }
                    }
                }
                if (line.toLowerCase().includes("project")) {
                    let elements = line.split(" ");
                    ShOm_QueryProcessor.ShOm_output += elements[0] + " " + elements[1] + "-->Cost is too small, neglect it\n";
                }
                if (line.toLowerCase().includes("groupby")) {
                    let elements = line.split(" ");
                    let groupByCost = 0;
                    if (elements[1] === "temp0") {
                        groupByCost = ShOm_QueryProcessor.ShOm_GetGroupByCost(ShOm_t1pages, ShOm_t3pages, ShOm_QueryProcessor.ShOm_isSortergeDone);
                    } else {
                        groupByCost = ShOm_QueryProcessor.ShOm_GetGroupByCost(tempTable, ShOm_t3pages, ShOm_QueryProcessor.ShOm_isSortergeDone);
                    }
                    totalShOm_QueryProcessoringCost += groupByCost;
                    ShOm_QueryProcessor.ShOm_output += elements[0] + " " + elements[1] + "-->Cost: " + groupByCost + "\n";
                }
            }
            ShOm_QueryProcessor.ShOm_output += "Total Disk I/O Cost is :- " + totalShOm_QueryProcessoringCost + "\n";
            ShOm_QueryProcessor.ShOm_output += "Query Processing Cost :- " + ShOm_QueryProcessor.ShOm_QueryProcessoringCost(totalShOm_QueryProcessoringCost) + "\n";
            return totalShOm_QueryProcessoringCost;
        } catch (ex) {
        }
        return 0;
    }

    static ShOm_TNLJoinCost(leftTable, rightTable, tuplesPerPage, obj) {
        try {
            let cost = (leftTable + tuplesPerPage * leftTable * rightTable);

            if ((cost >= 0)) {
                obj.joinCost = cost;
                obj.joinType = "TNL";
                obj.isReverseOrder = true;
            }

            console.log("\n---Tuple Nested Join---\n");
            console.log("\nTNL cost :- " + obj.joinCost);
            return cost;
        } catch (ex) {
        }
        return 0;
    }

    static ShOm_PNLJoinCost(leftTable, rightTable, obj) {
        try {
            let cost = leftTable + leftTable * rightTable;
            if (obj.joinCost > cost || ((obj.joinCost === 0) && cost > 0)) {
                obj.joinCost = cost;
                obj.joinType = "PNL";
            }

            console.log("\n---Page Nested Join---\n");
            console.log("PNL cost :- " + obj.joinCost);
            return cost;
        } catch (ex) {
        }
        return 0;
    }

    static ShOm_BNLJoinCostWithBuf(leftTable, rightTable, obj) {
        try {
            let ShOm_bufferMemory = ShOm_QueryProcessor.ShOm_ShOm_bufferMemoryWithBuff;
            let cost = (leftTable + (leftTable / ShOm_bufferMemory) * rightTable);

            if (obj.joinCost > cost || ((obj.joinCost === 0) && cost > 0)) {
                obj.joinCost = cost;
                obj.joinType = "BNL buffer = 50";
            }
            console.log("\n Block Nested Loop Join with buffer = 50 \n");
            console.log("BNL cost: " + obj.joinCost);
            return cost;
        } catch (ex) {
        }
        return 0;
    }

    static ShOm_BNLJoinCostWithLessBuf(leftTable, rightTable, obj) {
        try {
            let ShOm_bufferMemory = ShOm_QueryProcessor.ShOm_ShOm_bufferMemoryWithlessBuff;
            let cost = (leftTable + (leftTable / ShOm_bufferMemory) * rightTable);
            if (obj.joinCost > cost || ((obj.joinCost === 0) && cost > 0)) {
                obj.joinCost = cost;
                obj.joinType = "BNL buffer = 30";
                ShOm_QueryProcessor.ShOm_isSortergeDone = true;
                obj.isReverseOrder = false;
            }
            console.log("\nBlock Nested Loop Join with buffer = 30\n");
            console.log("BNL cost: " + obj.joinCost);
            return cost;
        } catch (ex) {
        }
        return 0;
    }

    static SShOm_MJJoinCostWithBuf(leftTable, rightTable, obj) {
        try {
            let cost;
            let ShOm_bufferMemory = ShOm_QueryProcessor.ShOm_ShOm_bufferMemoryWithBuff;
            let ShOm_mMultiplier = (Math.log10(leftTable / ShOm_bufferMemory)) / (Math.log10(ShOm_bufferMemory - 1));
            let ShOm_nMultiplier = (Math.log10(rightTable / ShOm_bufferMemory)) / (Math.log10(ShOm_bufferMemory - 1));
            cost = Math.ceil(
                2 * leftTable * (1 + ShOm_mMultiplier) + 2 * rightTable * (1 + ShOm_nMultiplier) + leftTable + rightTable);
            if (obj.joinCost > cost || ((obj.joinCost === 0) && cost > 0)) {
                obj.joinCost = cost;
                obj.joinType = "SMJ buffer = 50";
                obj.isReverseOrder = false;
                ShOm_QueryProcessor.ShOm_isSortergeDone = true;
            }
            console.log("\n Sort Merge Join with buffer = 50\n");
            console.log("SMJ cost: " + obj.joinCost);
            return cost;
        } catch (ex) {
        }
        return 0;
    }

    static ShOm_SMJJoinCostWithLessBuf(leftTable, rightTable, obj) {
        try {
            let cost;
            let ShOm_bufferMemory = ShOm_QueryProcessor.ShOm_ShOm_bufferMemoryWithlessBuff;
            let ShOm_mMultiplier = Math.log10(leftTable / ShOm_bufferMemory) / Math.log10(ShOm_bufferMemory - 1);
            let ShOm_nMultiplier = Math.log10(rightTable / ShOm_bufferMemory) / Math.log10(ShOm_bufferMemory - 1);
            cost = Math.ceil(
                (2 * leftTable * (1 + ShOm_mMultiplier) + 2 * rightTable * (1 + ShOm_nMultiplier) + leftTable + rightTable));
            if (obj.joinCost > cost || ((obj.joinCost === 0) && cost > 0)) {
                obj.joinCost = cost;
                obj.joinType = "SMJ buffer = " + ShOm_bufferMemory;
                obj.isReverseOrder = false;
                ShOm_QueryProcessor.ShOm_isSortergeDone = true;
            }
            console.log("\nSort Merge Join with less buffer = 30\n");
            console.log("SMJ cost: " + obj.joinCost);
            return cost;
        } catch (ex) {
        }
        return 0;
    }

    static ShOm_hashJoinCostWithBuffer(leftTable, rightTable, obj) {
        try {
            let ShOm_bufferMemory = ShOm_QueryProcessor.ShOm_ShOm_bufferMemoryWithBuff;
            let cost;
            let multiplier = Math.log10((leftTable + rightTable) / (ShOm_bufferMemory - 1))
                / (Math.log10(ShOm_bufferMemory - 1));
            cost = Math.ceil(2 * (leftTable + rightTable) * (1 + multiplier) + leftTable + rightTable);
            if (obj.joinCost > cost || ((obj.joinCost === 0) && cost > 0)) {
                obj.joinCost = cost;
                obj.joinType = "Hash Join buffer = " + ShOm_bufferMemory;
            }
            console.log("\n Hash Join with buffer = 50\n");
            console.log("HashJoin cost: " + obj.joinCost);
            return cost;
        } catch (ex) {
        }
        return 0;
    }

    static ShOm_hashJoinCostWithLessBuffer(leftTable, rightTable, obj) {
        try {
            let ShOm_bufferMemory = ShOm_QueryProcessor.ShOm_ShOm_bufferMemoryWithlessBuff;
            let multiplier = Math.log10((leftTable + rightTable) / (ShOm_bufferMemory - 1))
                / (Math.log10(ShOm_bufferMemory - 1));
            let cost;
            cost = Math.ceil(2 * (leftTable + rightTable) * (1 + multiplier) + leftTable + rightTable);
            if (obj.joinCost > cost || ((obj.joinCost === 0) && cost > 0)) {
                obj.joinCost = cost;
                obj.joinType = "Hash Join buffer = " + ShOm_bufferMemory;
            }
            console.log("\nHash Join with less buffer = 30\n");
            console.log("HashJoin cost: " + obj.joinCost);
            return cost;
        } catch (ex) {
        }
        return 0;
    }
}


ShOm_QueryProcessor.main();


