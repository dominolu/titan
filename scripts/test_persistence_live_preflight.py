"""Financial admission guard tests. Execute on 192.168.3.88 without account access."""
import copy
import unittest
from persistence_live_preflight import admitted, SYMBOL

class AdmissionTests(unittest.TestCase):
    def baseline(self):
        return [dict(symbol=SYMBOL,business_type='FUTURE',state='live',tick_size='0.1',lot_size='0.0001',min_size='0.0001'),dict(account_mode='CROSS_EXCHANGE',position_mode='SINGLE'),[],[],[],[],dict(bids=[{'p':'80670.8'}],asks=[{'p':'80670.9'}])]
    def test_frozen_policy_accepts_minimum_quote(self):
        value=admitted(*self.baseline());self.assertEqual(value['minimum_order_notional_quote'],'8.06709')
    def test_unit_drift_rejected(self):
        for name in ('tick_size','lot_size','min_size'):
            with self.subTest(name=name):
                values=self.baseline();values[0][name]='1'
                with self.assertRaises(AssertionError):admitted(*values)
    def test_wrong_symbol_or_business_or_state_rejected(self):
        for name,value in (('symbol','OTHER'),('business_type','SPOT'),('state','halt')):
            with self.subTest(name=name):
                values=self.baseline();values[0][name]=value
                with self.assertRaises(AssertionError):admitted(*values)
    def test_account_mode_drift_rejected(self):
        for name in ('account_mode','position_mode'):
            with self.subTest(name=name):
                values=self.baseline();values[1][name]='OTHER'
                with self.assertRaises(AssertionError):admitted(*values)
    def test_preexisting_risk_and_owner_rejected(self):
        for index in (2,3,4,5):
            with self.subTest(index=index):
                values=self.baseline();values[index]=[{'unexpected':'owner-or-exposure'}]
                with self.assertRaises(AssertionError):admitted(*values)
    def test_invalid_crossed_and_outside_grid_bbo_rejected(self):
        for bid,ask in (('0','1'),('80671','80670'),('120000','120001'),('50000','50001'),('Infinity','Infinity')):
            with self.subTest(bid=bid,ask=ask):
                values=self.baseline();values[-1]=dict(bids=[{'p':bid}],asks=[{'p':ask}])
                with self.assertRaises(AssertionError):admitted(*values)

if __name__=='__main__':unittest.main(verbosity=2)
