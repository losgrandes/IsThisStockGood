"""Tests for the MSNMoney.py functions."""


import os
import sys
import unittest

app_path = os.path.join(os.path.dirname(__file__), "..", 'src')
sys.path.append(app_path)

from src.MSNMoney import MSNMoneyKeyRatios

class MSNMoneyTest(unittest.TestCase):


  def test_parse_pe_ratios_should_properly_calculate_pe_ratios(self):
    msn = MSNMoneyKeyRatios('DUMMY')
    payload = {
      'companyMetrics' : [
        {
          'fiscalPeriodType' : 'Annual',
          'priceToEarningsRatio' : 0.5,
          'year': 2021
        },
        {
          'fiscalPeriodType' : 'Annual',
          'priceToEarningsRatio' : 103.5,
          'year': 2020
        },
        {
          'fiscalPeriodType' : 'Annual',
          'priceToEarningsRatio' : 21.5,
          'year': 2019
        },
        {
          'fiscalPeriodType' : 'Annual',
          'priceToEarningsRatio' : 43.7,
          'year': 2018
        },
        {
          'fiscalPeriodType' : 'Annual',
          'priceToEarningsRatio' : 1.5,
          'year': 2017
        },
        {
          'fiscalPeriodType' : 'Annual',
          'priceToEarningsRatio' : 22.5,
          'year': 2016
        },
        {
          'fiscalPeriodType' : 'Annual',
          'priceToEarningsRatio' : 50.9,
          'year': 2015
        },
      ]
    }
    self.assertTrue(msn._parse_annual_ratios(payload))
    self.assertEqual(msn.pe_high, 103.5)
    self.assertEqual(msn.pe_low, 0.5)
