package com.maxrybalkin91.transactions.repository

import com.maxrybalkin91.transactions.util.DbSettings.DB_PASSWORD
import com.maxrybalkin91.transactions.util.DbSettings.DB_URL
import com.maxrybalkin91.transactions.util.DbSettings.DB_USER
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import java.sql.Connection
import java.sql.DriverManager

/**
 * "Read Uncommited" isolation level cases
 */
@SpringBootTest
open class ReadUncommitedTests {
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
     * There is 500$ on the bank account. A man in a bank office
     * Two people transfer 100$ simultaneously
     * One transfer is commited, one is done with rollback because of lost connection
     *
     * But we successfully read uncommited data before it's abortion
     * Therefore, we will see 700$ instead of 600$
     * We just gave 100$ for charity
     */
    @Test
    @DirtiesContext
    fun `reading, updating in both T1 and T2, only T2 result is seen after`() {
        val conn1 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        val conn2 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        try {
            conn1.autoCommit = false
            conn1.transactionIsolation = Connection.TRANSACTION_READ_UNCOMMITTED
            conn2.autoCommit = false
            conn2.transactionIsolation = Connection.TRANSACTION_READ_UNCOMMITTED

            val balanceRead1 = conn1.prepareStatement("select balance from account where id = 1").executeQuery()
            balanceRead1.next()
            addToBalance(conn1, balanceRead1.getInt(1) + 100)

            val balanceRead2 = conn2.prepareStatement("select balance from account where id = 1").executeQuery()
            balanceRead2.next()
            conn1.rollback()
            addToBalance(conn2, balanceRead2.getInt(1) + 100)
            conn2.commit()

            Assertions.assertThrows(AssertionError::class.java) {
                val balanceRead3 = conn1.prepareStatement("select balance from account where id = 1").executeQuery()
                Assertions.assertTrue(balanceRead3.next())
                Assertions.assertTrue(balanceRead3.getInt(1) < 700)
            }
        } catch (e: Exception) {
            Assertions.fail("Unexpected exception", e)
        } finally {
            conn1.close()
        }
    }

    private fun addToBalance(connection: Connection, balance: Int) {
        val update = connection.prepareStatement("update account set balance = $balance where id = 1")
        update.executeUpdate()
    }
}