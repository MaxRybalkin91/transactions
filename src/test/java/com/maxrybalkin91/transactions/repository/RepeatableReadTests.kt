package com.maxrybalkin91.transactions.repository

import com.maxrybalkin91.transactions.util.DbSettings.DB_PASSWORD
import com.maxrybalkin91.transactions.util.DbSettings.DB_URL
import com.maxrybalkin91.transactions.util.DbSettings.DB_USER
import org.h2.jdbc.JdbcSQLTransactionRollbackException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import java.sql.Connection
import java.sql.DriverManager

/**
 * "Repeatable read" isolation level cases
 */
@SpringBootTest
open class RepeatableReadTests {
    /**
     * One manager wants an employee with today birthday be additionally paid
     * Another manager adds and employee who both started working and has a birthday today
     *
     * Due to seeing phantom reads, both employees get the payment, despite the second one
     * doesn't deserve it since he hasn't been working long period yet
     *
     * That happens because 'repeatable read' doesn't protect from phantom reads
     */
    @Test
    @DirtiesContext
    fun `updating in T1, and catching inserts from T2`() {
        val conn1 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        val conn2 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        try {
            //COMMITS AUTOMATICALLY
            conn1.autoCommit = true
            conn2.prepareStatement("insert into employee(id, birthday, bonus) values (1, '2000-01-01', null)")
                .executeUpdate()

            conn1.transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ

            //"BEGIN TRANSACTION" starts right here
            conn1.autoCommit = false

            conn2.prepareStatement("insert into employee(id, birthday, bonus) values (2, '2000-01-01', null)")
                .executeUpdate()

            val searchingRead = conn1.prepareStatement("select id from employee where birthday = '2000-01-01'")
                .executeQuery()
            val ids = arrayListOf<Int>()
            while (searchingRead.next()) {
                ids.add(searchingRead.getInt(1))
            }
            Assertions.assertFalse(ids.size == 1)
        } catch (e: Exception) {
            Assertions.fail("Unexpected exception", e)
        } finally {
            conn1?.close()
            conn2?.close()
        }
    }

    /**
     * 2 users want to book a room in a hotel.
     * They start reading and booking simultaneously
     * But after first user books the number, the second booking fails
     *
     * "Repeatable read" puts updates and deletes in a queue, and if one of them is commited,
     * the second attempt would be aborted
     */
    @Test
    @DirtiesContext
    fun `reading in T1 and T2, updating in T1, failure during updating it T1`() {
        val conn1 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        val conn2 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        val conn3 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        try {
            conn1.transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ
            conn2.transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ

            conn3.autoCommit = true

            //"BEGIN TRANSACTION" starts right here, not in the first read
            conn1.autoCommit = false
            conn2.autoCommit = false

            //COMMITS AUTOMATICALLY
            conn3.prepareStatement("insert into room(id, is_available) values (1, true)").executeUpdate()

            val firstRead = conn1.prepareStatement("select * from room where id = 1").executeQuery()
            val secondRead = conn2.prepareStatement("select * from room where id = 1").executeQuery()

            Assertions.assertTrue(firstRead.next())
            Assertions.assertTrue(firstRead.getBoolean(2))
            val expectedGuestName = "Andrew"
            val preparedStatement =
                conn1.prepareStatement("update room set is_available = false, guest_name = ? where id = 1")
            preparedStatement.setString(1, expectedGuestName)
            preparedStatement.executeUpdate()
            conn1.commit()

            Assertions.assertTrue(secondRead.next())
            Assertions.assertTrue(secondRead.getBoolean(2))
            Assertions.assertThrows(JdbcSQLTransactionRollbackException::class.java) {
                conn2.prepareStatement("update room set is_available = false, guest_name = 'John' where id = 1")
                    .executeUpdate()
            }

            val finalResult = conn3.prepareStatement("select guest_name from room where id = 1").executeQuery()
            Assertions.assertTrue(finalResult.next())
            Assertions.assertTrue(finalResult.getString(1) == expectedGuestName)
        } catch (e: Exception) {
            Assertions.fail("Unexpected exception", e)
        } finally {
            conn1?.close()
            conn2?.close()
            conn3?.close()
        }
    }
}