package com.maxrybalkin91.transactions.repository

import com.maxrybalkin91.transactions.util.DbSettings.DB_PASSWORD
import com.maxrybalkin91.transactions.util.DbSettings.DB_URL
import com.maxrybalkin91.transactions.util.DbSettings.DB_USER
import org.h2.jdbc.JdbcSQLTimeoutException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

/**
 * "Read Commited" isolation level cases
 */
@SpringBootTest
open class ReadCommitedTests {
    @BeforeEach
    fun fillDb() {
        DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD).use {
            it.prepareStatement("delete from account where id = 1")
                .executeUpdate()
            it.prepareStatement("insert into account(id, balance) values(1, 500)")
                .executeUpdate()
            it.commit()
            it.close()
        }
    }

    @Test
    @Order(1)
    fun checkNotEmptyDb() {
        DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD).use {
            Assertions.assertTrue(
                it.prepareStatement("select * from account")
                    .executeQuery()
                    .next()
            )
        }
    }

    /**
     * There is 500$ on the bank account. A man checks it on an ATM
     * and then withdraws 250$ of cash. At the same time his wife checks
     * the balance on the mobile app as 500$, and decides to spend 250$ on Amazon.
     *
     * Due to bad application's architecture, it executes update with data was read before.
     *
     * Man's cash withdrawal transaction commit happens right after wife's balance checking.
     * Wife's transaction commits and updates the balance as (!) HER APP 500$ shown - 250$ transaction.
     *
     * Now the balance on the account is 250$ instead of 0$
     * (but we had two 250$ transactions, not one)
     *
     * The bank just lost 250$.
     */
    @Test
    @DirtiesContext
    fun `lost update due to commiting T1 after reading in T2`() {
        val conn1 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        val conn2 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        try {
            conn1.autoCommit = false
            conn2.autoCommit = false

            val rs1: ResultSet = getAccount(conn1)
            if (rs1.next()) {
                reduceBalance(conn1, rs1.getInt("balance") - 250)
            }

            val rs2: ResultSet = getAccount(conn2)
            if (rs2.next()) {
                conn1.commit()
                reduceBalance(conn2, rs2.getInt("balance") - 250)
                conn2.commit()
            }
            DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD).use { conn3 ->
                val readStmt = conn3.prepareStatement("select * from account where id = ?")
                readStmt.setInt(1, 1)
                val rs = readStmt.executeQuery()
                if (rs.next()) {
                    val currentBalance = rs.getInt("balance")
                    Assertions.assertThrows(AssertionError::class.java) {
                        Assertions.assertEquals(0, currentBalance)
                    }
                } else {
                    Assertions.fail<Any>("No account found")
                }
            }
        } catch (e: Exception) {
            Assertions.fail("Unexpected exception", e)
        } finally {
            conn1.close()
            conn2.close()
        }
    }

    /**
     * A customer checks if there is an item available at an online store.
     * His first request shows there is nothing.
     *
     * In a few minutes a manager adds it as available into the DB.
     * Another customer opens the website and orders the same item.
     *
     * During the second's customer actions, the first customer also sees the item available and orders it.
     *
     * As a result, the first customer won't get his item, and in the DB the quantity will be negative.
     */
    @Test
    @DirtiesContext
    fun `reading in T1, inserting and updating in T2, reading in T1 again`() {
        val conn1 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        val conn2 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        val conn3 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        try {
            conn1.autoCommit = false
            conn2.autoCommit = false
            conn3.autoCommit = false

            val selectStatement = "select * from items where id = 1 and quantity > 0"

            var firstCustomerReading = conn1
                .prepareStatement(selectStatement)
                .executeQuery()
            Assertions.assertTrue(!firstCustomerReading.next())

            //Reading of the first customer is closed
            conn1.commit()

            conn3.prepareStatement("insert into items(id, quantity) values(1, 1)").executeUpdate()
            conn3.commit()

            val secondCustomerReading = conn2
                .prepareStatement(selectStatement)
                .executeQuery()
            Assertions.assertTrue(secondCustomerReading.next())

            conn2.prepareStatement("update items set quantity = quantity - 1 where id = 1")
                .executeUpdate()

            firstCustomerReading = conn1
                .prepareStatement(selectStatement)
                .executeQuery()
            Assertions.assertTrue(firstCustomerReading.next())

            conn2.commit()

            conn1.prepareStatement("update items set quantity = quantity - 1 where id = 1")
                .executeUpdate()
            conn1.commit()

            Assertions.assertThrows(AssertionError::class.java) {
                DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD).use {
                    val result = it.prepareStatement("select quantity from items where id = 1")
                        .executeQuery()
                    Assertions.assertTrue(result.next())
                    Assertions.assertTrue(result.getInt(1) >= 0)
                }
            }
        } catch (e: Exception) {
            Assertions.fail("Unexpected exception", e)
        } finally {
            conn1.close()
            conn2.close()
            conn3.close()
        }
    }

    //THERE ARE POSITIVE SCENARIOS TOO:)

    /**
     * There is 500$ on the bank account. A man in a bank office
     * getting all the money from the account (500$).
     * At the same time his wife is in another office adds 1000$ to the account.
     *
     * Now to add 1000$ we use SQL updating.
     *
     * Since the first transaction isn't commited yet, updating isn't possible.
     * ReadCommited level blocks a row from other transaction updates
     *
     * Having 1500$ in temporary impossible; at least not possible in the first wife's reading
     */
    @Test
    @DirtiesContext
    fun `reading in T1, updating and deleting in T2, and updating in T1 via SQL`() {
        val conn1 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        val conn2 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        try {
            conn1.autoCommit = false
            conn2.autoCommit = false

            val accRead1 = getAccount(conn1)
            val accRead2 = getAccount(conn2)

            accRead1.next()
            accRead2.next()

            val balanceRead1 = accRead1.getInt(1)
            val balanceRead2 = accRead1.getInt(1)

            reduceBalance(conn1, balanceRead1)

            Assertions.assertThrows(JdbcSQLTimeoutException::class.java) {
                conn2.prepareStatement("update account set balance = ${balanceRead2 + 1000} where id = 1")
                    .executeUpdate()
            }
        } catch (e: Exception) {
            Assertions.fail("Unexpected exception", e)
        } finally {
            conn1.close()
            conn2.close()
        }
    }

    /**
     * There is 500$ on the bank account. A man in a bank office
     * getting all the money from the account (500$).
     * At the same time his wife is in another office adds 1000$ to the account.
     *
     * To add 1000$ we use transactional updating.
     *
     * Since the first transaction isn't commited yet, updating isn't possible.
     * ReadCommited level blocks a row from other transaction updates
     *
     * Having 1500$ in temporary impossible; at least not possible in the first wife's reading
     */
    @Test
    @DirtiesContext
    fun `reading in T1, updating and deleting in T2, and updating in T1 via Spring Transaction`() {
        val conn1 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        val conn2 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        try {
            conn1.autoCommit = false
            conn2.autoCommit = false

            val accRead1 = getAccount(conn1)
            Assertions.assertTrue(accRead1.next())
            val balanceRead1 = accRead1.getInt(1)

            val accRead2 = getAccount(conn2)
            Assertions.assertTrue(accRead2.next())
            var balanceRead2 = accRead2.getInt(1)

            reduceBalance(conn1, balanceRead1)

            balanceRead2 += 1000

            Assertions.assertThrows(JdbcSQLTimeoutException::class.java) {
                conn2.prepareStatement("update account set balance = $balanceRead2 where id = 1")
                    .executeUpdate()
            }
        } catch (e: Exception) {
            Assertions.fail("Unexpected exception", e)
        } finally {
            conn1.rollback()
            conn1.close()
            conn2.rollback()
            conn2.close()
        }
    }

    private fun getAccount(connection: Connection): ResultSet =
        connection.prepareStatement("select * from account where id = 1").executeQuery()

    private fun reduceBalance(connection: Connection, balance: Int) {
        val update = connection.prepareStatement("update account set balance = ? where id = 1")
        update.setInt(1, balance)
        update.executeUpdate()
    }
}