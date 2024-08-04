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
     * 'Serializable' prevent phantom reads, so second time reading doesn't show a new one
     */
    @Test
    @DirtiesContext
    fun `updating in T1, and catching inserts from T2`() {
        val conn1 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        val conn2 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        val conn3 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        try {
            conn3.autoCommit = true
            conn3.prepareStatement("insert into employee(id, birthday) values (1, '2000-01-01')")
                .executeUpdate()

            conn1.transactionIsolation = Connection.TRANSACTION_SERIALIZABLE
            conn1.autoCommit = false
            conn2.autoCommit = false

            val firstSearchRead = conn1.prepareStatement("select id from employee where birthday = '2000-01-01'")
                .executeQuery()
            val firstSearchIds = arrayListOf<Int>()
            while (firstSearchRead.next()) {
                firstSearchIds.add(firstSearchRead.getInt(1))
            }

            conn2.prepareStatement("insert into employee(id, birthday) values (2, '2000-01-01')")
                .executeUpdate()
            conn2.commit()

            val secondSearchRead = conn1.prepareStatement("select id from employee where birthday = '2000-01-01'")
                .executeQuery()
            conn1.commit()
            val secondSearchIds = arrayListOf<Int>()
            while (secondSearchRead.next()) {
                secondSearchIds.add(secondSearchRead.getInt(1))
            }
            Assertions.assertTrue(firstSearchIds.size == secondSearchIds.size)
        } catch (e: Exception) {
            Assertions.fail("Unexpected exception", e)
        } finally {
            conn1?.close()
            conn2?.close()
            conn3?.close()
        }
    }
}