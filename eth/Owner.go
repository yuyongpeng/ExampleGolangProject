// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package eth

import (
	"errors"
	"math/big"
	"strings"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = errors.New
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// EthMetaData contains all meta data concerning the Eth contract.
var EthMetaData = &bind.MetaData{
	ABI: "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"oldOwner\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"newOwner\",\"type\":\"address\"}],\"name\":\"OwnerSet\",\"type\":\"event\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"newOwner\",\"type\":\"address\"}],\"name\":\"changeOwner\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"getOwner\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]",
}

// EthABI is the input ABI used to generate the binding from.
// Deprecated: Use EthMetaData.ABI instead.
var EthABI = EthMetaData.ABI

// Eth is an auto generated Go binding around an Ethereum contract.
type Eth struct {
	EthCaller     // Read-only binding to the contract
	EthTransactor // Write-only binding to the contract
	EthFilterer   // Log filterer for contract events
}

// EthCaller is an auto generated read-only Go binding around an Ethereum contract.
type EthCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// EthTransactor is an auto generated write-only Go binding around an Ethereum contract.
type EthTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// EthFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type EthFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// EthSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type EthSession struct {
	Contract     *Eth              // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// EthCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type EthCallerSession struct {
	Contract *EthCaller    // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// EthTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type EthTransactorSession struct {
	Contract     *EthTransactor    // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// EthRaw is an auto generated low-level Go binding around an Ethereum contract.
type EthRaw struct {
	Contract *Eth // Generic contract binding to access the raw methods on
}

// EthCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type EthCallerRaw struct {
	Contract *EthCaller // Generic read-only contract binding to access the raw methods on
}

// EthTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type EthTransactorRaw struct {
	Contract *EthTransactor // Generic write-only contract binding to access the raw methods on
}

// NewEth creates a new instance of Eth, bound to a specific deployed contract.
func NewEth(address common.Address, backend bind.ContractBackend) (*Eth, error) {
	contract, err := bindEth(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Eth{EthCaller: EthCaller{contract: contract}, EthTransactor: EthTransactor{contract: contract}, EthFilterer: EthFilterer{contract: contract}}, nil
}

// NewEthCaller creates a new read-only instance of Eth, bound to a specific deployed contract.
func NewEthCaller(address common.Address, caller bind.ContractCaller) (*EthCaller, error) {
	contract, err := bindEth(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &EthCaller{contract: contract}, nil
}

// NewEthTransactor creates a new write-only instance of Eth, bound to a specific deployed contract.
func NewEthTransactor(address common.Address, transactor bind.ContractTransactor) (*EthTransactor, error) {
	contract, err := bindEth(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &EthTransactor{contract: contract}, nil
}

// NewEthFilterer creates a new log filterer instance of Eth, bound to a specific deployed contract.
func NewEthFilterer(address common.Address, filterer bind.ContractFilterer) (*EthFilterer, error) {
	contract, err := bindEth(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &EthFilterer{contract: contract}, nil
}

// bindEth binds a generic wrapper to an already deployed contract.
func bindEth(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(EthABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Eth *EthRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Eth.Contract.EthCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Eth *EthRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Eth.Contract.EthTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Eth *EthRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Eth.Contract.EthTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Eth *EthCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Eth.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Eth *EthTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Eth.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Eth *EthTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Eth.Contract.contract.Transact(opts, method, params...)
}

// GetOwner is a free data retrieval call binding the contract method 0x893d20e8.
//
// Solidity: function getOwner() view returns(address)
func (_Eth *EthCaller) GetOwner(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _Eth.contract.Call(opts, &out, "getOwner")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// GetOwner is a free data retrieval call binding the contract method 0x893d20e8.
//
// Solidity: function getOwner() view returns(address)
func (_Eth *EthSession) GetOwner() (common.Address, error) {
	return _Eth.Contract.GetOwner(&_Eth.CallOpts)
}

// GetOwner is a free data retrieval call binding the contract method 0x893d20e8.
//
// Solidity: function getOwner() view returns(address)
func (_Eth *EthCallerSession) GetOwner() (common.Address, error) {
	return _Eth.Contract.GetOwner(&_Eth.CallOpts)
}

// ChangeOwner is a paid mutator transaction binding the contract method 0xa6f9dae1.
//
// Solidity: function changeOwner(address newOwner) returns()
func (_Eth *EthTransactor) ChangeOwner(opts *bind.TransactOpts, newOwner common.Address) (*types.Transaction, error) {
	return _Eth.contract.Transact(opts, "changeOwner", newOwner)
}

// ChangeOwner is a paid mutator transaction binding the contract method 0xa6f9dae1.
//
// Solidity: function changeOwner(address newOwner) returns()
func (_Eth *EthSession) ChangeOwner(newOwner common.Address) (*types.Transaction, error) {
	return _Eth.Contract.ChangeOwner(&_Eth.TransactOpts, newOwner)
}

// ChangeOwner is a paid mutator transaction binding the contract method 0xa6f9dae1.
//
// Solidity: function changeOwner(address newOwner) returns()
func (_Eth *EthTransactorSession) ChangeOwner(newOwner common.Address) (*types.Transaction, error) {
	return _Eth.Contract.ChangeOwner(&_Eth.TransactOpts, newOwner)
}

// EthOwnerSetIterator is returned from FilterOwnerSet and is used to iterate over the raw logs and unpacked data for OwnerSet events raised by the Eth contract.
type EthOwnerSetIterator struct {
	Event *EthOwnerSet // Event containing the contract specifics and raw log

	contract *bind.BoundContract // Generic contract to use for unpacking event data
	event    string              // Event name to use for unpacking event data

	logs chan types.Log        // Log channel receiving the found contract events
	sub  ethereum.Subscription // Subscription for errors, completion and termination
	done bool                  // Whether the subscription completed delivering logs
	fail error                 // Occurred error to stop iteration
}

// Next advances the iterator to the subsequent event, returning whether there
// are any more events found. In case of a retrieval or parsing error, false is
// returned and Error() can be queried for the exact failure.
func (it *EthOwnerSetIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(EthOwnerSet)
			if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
				it.fail = err
				return false
			}
			it.Event.Raw = log
			return true

		default:
			return false
		}
	}
	// Iterator still in progress, wait for either a data or an error event
	select {
	case log := <-it.logs:
		it.Event = new(EthOwnerSet)
		if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
			it.fail = err
			return false
		}
		it.Event.Raw = log
		return true

	case err := <-it.sub.Err():
		it.done = true
		it.fail = err
		return it.Next()
	}
}

// Error returns any retrieval or parsing error occurred during filtering.
func (it *EthOwnerSetIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *EthOwnerSetIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// EthOwnerSet represents a OwnerSet event raised by the Eth contract.
type EthOwnerSet struct {
	OldOwner common.Address
	NewOwner common.Address
	Raw      types.Log // Blockchain specific contextual infos
}

// FilterOwnerSet is a free log retrieval operation binding the contract event 0x342827c97908e5e2f71151c08502a66d44b6f758e3ac2f1de95f02eb95f0a735.
//
// Solidity: event OwnerSet(address indexed oldOwner, address indexed newOwner)
func (_Eth *EthFilterer) FilterOwnerSet(opts *bind.FilterOpts, oldOwner []common.Address, newOwner []common.Address) (*EthOwnerSetIterator, error) {

	var oldOwnerRule []interface{}
	for _, oldOwnerItem := range oldOwner {
		oldOwnerRule = append(oldOwnerRule, oldOwnerItem)
	}
	var newOwnerRule []interface{}
	for _, newOwnerItem := range newOwner {
		newOwnerRule = append(newOwnerRule, newOwnerItem)
	}

	logs, sub, err := _Eth.contract.FilterLogs(opts, "OwnerSet", oldOwnerRule, newOwnerRule)
	if err != nil {
		return nil, err
	}
	return &EthOwnerSetIterator{contract: _Eth.contract, event: "OwnerSet", logs: logs, sub: sub}, nil
}

// WatchOwnerSet is a free log subscription operation binding the contract event 0x342827c97908e5e2f71151c08502a66d44b6f758e3ac2f1de95f02eb95f0a735.
//
// Solidity: event OwnerSet(address indexed oldOwner, address indexed newOwner)
func (_Eth *EthFilterer) WatchOwnerSet(opts *bind.WatchOpts, sink chan<- *EthOwnerSet, oldOwner []common.Address, newOwner []common.Address) (event.Subscription, error) {

	var oldOwnerRule []interface{}
	for _, oldOwnerItem := range oldOwner {
		oldOwnerRule = append(oldOwnerRule, oldOwnerItem)
	}
	var newOwnerRule []interface{}
	for _, newOwnerItem := range newOwner {
		newOwnerRule = append(newOwnerRule, newOwnerItem)
	}

	logs, sub, err := _Eth.contract.WatchLogs(opts, "OwnerSet", oldOwnerRule, newOwnerRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(EthOwnerSet)
				if err := _Eth.contract.UnpackLog(event, "OwnerSet", log); err != nil {
					return err
				}
				event.Raw = log

				select {
				case sink <- event:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

// ParseOwnerSet is a log parse operation binding the contract event 0x342827c97908e5e2f71151c08502a66d44b6f758e3ac2f1de95f02eb95f0a735.
//
// Solidity: event OwnerSet(address indexed oldOwner, address indexed newOwner)
func (_Eth *EthFilterer) ParseOwnerSet(log types.Log) (*EthOwnerSet, error) {
	event := new(EthOwnerSet)
	if err := _Eth.contract.UnpackLog(event, "OwnerSet", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
