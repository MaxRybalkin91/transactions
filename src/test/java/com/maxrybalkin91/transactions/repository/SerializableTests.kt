package com.maxrybalkin91.transactions.repository

import com.maxrybalkin91.transactions.util.DbSettings.DB_PASSWORD
import com.maxrybalkin91.transactions.util.DbSettings.DB_URL
import com.maxrybalkin91.transactions.util.DbSettings.DB_USER
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import java.sql.Connection
import java.sql.DriverManager

/**
 * "Serializable" isolation level cases
 */
@SpringBootTest
open class SerializableTests {
    /**
     * One manager wants an employee with today birthday be additionally paid
     * Another manager adds and employee who both started working and has a birthday today
     * 'Serializable' prevent phantom reads, so only the first employee will be marked as "bonus-deserved"
     */
    @Test
    @DirtiesContext
    fun `updating in T1, and catching inserts from T2`() {
        val conn1 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        val conn2 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        val conn3 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        try {
            conn3.autoCommit = true
            conn3.prepareStatement("insert into employee(id, birthday, bonus) values (1, '2000-01-01', null)")
                .executeUpdate()

            conn1.transactionIsolation = Connection.TRANSACTION_SERIALIZABLE
            conn2.transactionIsolation = Connection.TRANSACTION_SERIALIZABLE

            conn1.autoCommit = false

            conn2.autoCommit = false
            conn2.prepareStatement("insert into employee(id, birthday, bonus) values (2, '2000-01-01', null)")
                .executeUpdate()
            conn2.commit()

            val searchingRead = conn1.prepareStatement("select id from employee where birthday = '2000-01-01'")
                .executeQuery()
            conn1.commit()
            val ids = arrayListOf<Int>()
            while (searchingRead.next()) {
                ids.add(searchingRead.getInt(1))
            }
            //Prove only one matching row
            Assertions.assertTrue(ids.size == 1)
        } catch (e: Exception) {
            Assertions.fail("Unexpected exception", e)
        } finally {
            conn1?.close()
            conn2?.close()
            conn3?.close()
        }
    }
}